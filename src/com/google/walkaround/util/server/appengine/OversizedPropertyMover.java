/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.walkaround.util.server.appengine;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Moves data from entity Blob properties into Blobstore blobs and back when a
 * size limit is exceeded, mostly transparently.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class OversizedPropertyMover {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(OversizedPropertyMover.class.getName());

  private static final int MAX_FILE_BYTES_TRANSFERRED_PER_RPC = 972800;
  private static final int BUFFER_BYTES = MAX_FILE_BYTES_TRANSFERRED_PER_RPC;

  /**
   * Wrapper around {@link FileWriteChannel} that turns calls to {@link #close}
   * into calls to {@link FileWriteChannel#closeFinally}.
   */
  private static class CloseFinallyChannel implements FileWriteChannel {
    private final FileWriteChannel delegate;

    private CloseFinallyChannel(FileWriteChannel delegate) {
      this.delegate = delegate;
    }

    @Override public String toString() {
      return "CloseFinallyChannel(" + delegate + ")";
    }

    @Override public void close() throws IOException {
      closeFinally();
    }

    @Override public void closeFinally() throws IOException {
      delegate.closeFinally();
    }

    @Override public int write(ByteBuffer src) throws IOException {
      return delegate.write(src);
    }

    @Override public int write(ByteBuffer src, String sequenceKey) throws IOException {
      return delegate.write(src, sequenceKey);
    }

    @Override public boolean isOpen() {
      return delegate.isOpen();
    }
  }

  private static OutputStream openForFinalWrite(AppEngineFile file) throws IOException {
    return Channels.newOutputStream(
        new CloseFinallyChannel(getFileService().openWriteChannel(file, true)));
  }

  public static FileService getFileService() {
    return FileServiceFactory.getFileService();
  }

  private static final String MIME_TYPE = "application/vnd.appengine.oversized-entity-property";

  private static String abbrev(String s) {
    return s.length() <= 300 ? s : s.substring(0, 300) + "...";
  }

  private static AppEngineFile newFile(String fileDescription) throws IOException {
    return getFileService().createNewBlobFile(MIME_TYPE, abbrev(fileDescription));
  }

  private static byte[] slurp(BlobKey blobKey) throws IOException {
    FileReadChannel in = getFileService().openReadChannel(
        new AppEngineFile(AppEngineFile.FileSystem.BLOBSTORE, blobKey.getKeyString()), 
        false);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteBuffer buf = ByteBuffer.allocate(BUFFER_BYTES);
    while (true) {
      int bytesRead = in.read(buf);
      if (bytesRead < 0) {
        break;
      }
      Preconditions.checkState(bytesRead != 0, "0 bytes read: %s", buf);
      out.write(buf.array(), 0, bytesRead);
      buf.clear();
    }
    return out.toByteArray();
  }

  public interface BlobWriteListener {
    void blobCreated(Key entityKey, MovableProperty property, BlobKey blobKey);
    void blobDeleted(Key entityKey, MovableProperty property, BlobKey blobKey);
  }

  public static final BlobWriteListener NULL_LISTENER = new BlobWriteListener() {
    @Override public void blobCreated(Key entityKey, MovableProperty property, BlobKey blobKey) {}
    @Override public void blobDeleted(Key entityKey, MovableProperty property, BlobKey blobKey) {}
  };

  public static final class MovableProperty {
    public static enum PropertyType {
      TEXT, BLOB;
    }

    private final String propertyName;
    private final String movedPropertyName;
    private final PropertyType type;

    public MovableProperty(String propertyName,
        String movedPropertyName,
        PropertyType type) {
      this.propertyName = checkNotNull(propertyName, "Null propertyName");
      this.movedPropertyName = checkNotNull(movedPropertyName, "Null movedPropertyName");
      this.type = checkNotNull(type, "Null type");
    }

    public String getPropertyName() {
      return propertyName;
    }

    public String getMovedPropertyName() {
      return movedPropertyName;
    }

    public PropertyType getType() {
      return type;
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "("
          + propertyName + ", "
          + movedPropertyName + ", "
          + type
          + ")";
    }
  }

  // TODO(ohler): Be smarter if there are multiple properties that can be moved,
  // and perhaps make configurable.
  private static final int MAX_INLINE_VALUE_BYTES = 500 * 1000;

  private final CheckedDatastore datastore;
  private final List<MovableProperty> properties;
  private final BlobWriteListener listener;

  public OversizedPropertyMover(CheckedDatastore datastore,
      List<MovableProperty> properties,
      BlobWriteListener listener) {
    this.datastore = checkNotNull(datastore, "Null datastore");
    this.properties = ImmutableList.copyOf(properties);
    this.listener = checkNotNull(listener, "Null listener");
  }

  private BlobKey getBlobKey(final AppEngineFile finalizedBlobFile) 
      throws IOException {
    try {
      return new RetryHelper().run(
          new RetryHelper.Body<BlobKey>() {
            @Override public BlobKey run() throws RetryableFailure, PermanentFailure {
              // HACK(ohler): The file service incorrectly uses the current
              // transaction.  Make a dummy transaction as a workaround.
              // Apparently it even needs to be XG.
              CheckedTransaction tx = datastore.beginTransactionXG();
              try {
                BlobKey key = getFileService().getBlobKey(finalizedBlobFile);
                if (key == null) {
                  // I have the impression that this can happen because of HRD's
                  // eventual consistency.  Retry.
                  throw new RetryableFailure(this + ": getBlobKey() returned null");
                }
                return key;
              } finally {
                tx.close();
              }
            }
          });
    } catch (PermanentFailure e) {
      throw new IOException("Failed to get blob key for " + finalizedBlobFile, e);
    }
  }

  private BlobKey dump(String fileDescription, byte[] bytes) throws IOException {
    AppEngineFile file = newFile(fileDescription);
    OutputStream out = openForFinalWrite(file);
    out.write(bytes);
    out.close();
    BlobKey blobKey = getBlobKey(file);
    // We verify if what's in the file matches what we wanted to write -- the
    // Files API is still experimental and I've seen it lose data.
    byte[] actualContent = slurp(blobKey);
    if (!Arrays.equals(bytes, actualContent)) {
      throw new IOException("File " + file + " does not contain the bytes we intended to write");
    }
    return blobKey;
  }

  private byte[] getBytes(ByteBuffer in) {
    if (in.hasArray() && in.position() == 0
        && in.arrayOffset() == 0 && in.array().length == in.limit()) {
      return in.array();
    } else {
      byte[] buf = new byte[in.remaining()];
      in.get(buf);
      return buf;
    }
  }

  private byte[] encode(String s) {
    try {
      ByteBuffer buffer = Charsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(s));
      return getBytes(buffer);
    } catch (CharacterCodingException e) {
      throw new RuntimeException("Failed to encode string " + abbrev(s), e);
    }
  }

  private String decode(byte[] bytes) {
    try {
      return Charsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes))
          .toString();
    } catch (CharacterCodingException e) {
      throw new RuntimeException("Failed to decode bytes", e);
    }
  }

  public void prePut(Entity entity) {
    Preconditions.checkNotNull(entity, "Null entity");
    for (MovableProperty property : properties) {
      Preconditions.checkArgument(!entity.hasProperty(property.getMovedPropertyName()),
          "Entity %s already has moved property %s", entity, property);
      if (entity.hasProperty(property.getPropertyName())) {
        Object o = entity.getProperty(property.getPropertyName());
        byte[] bytes;
        switch (property.getType()) {
          case BLOB:
            Preconditions.checkArgument(o instanceof Blob,
                "%s: Property %s not Blob: %s", entity, property, o);
            bytes = ((Blob) o).getBytes();
            break;
          case TEXT:
            Preconditions.checkArgument(o instanceof Text,
                "%s: Property %s not Text: %s", entity, property, o);
            bytes = encode(((Text) o).getValue());
            break;
          default:
            throw new RuntimeException("Unexpected type in " + property);
        }
        if (bytes.length > MAX_INLINE_VALUE_BYTES) {
          BlobKey blobKey;
          try {
            blobKey = dump(
                "Oversized value of " + property + " for entity " + entity.getKey(),
                bytes);
          } catch (ApiProxyException e) {
            throw new RuntimeException("Failed to dump " + property + " to file: " + entity, e);
          } catch (IOException e) {
            throw new RuntimeException("Failed to dump " + property + " to file: " + entity, e);
          }
          log.info("Moved " + bytes.length + " bytes from " + property + " to " + blobKey);
          listener.blobCreated(entity.getKey(), property, blobKey);
          entity.removeProperty(property.getPropertyName());
          // We deliberately leave this indexable.  This way, a hypothetical
          // garbage collection algorithm could use the single-property index to
          // find out which blob keys are referenced.  Maybe this makes the
          // listener mechanism redundant...
          entity.setProperty(property.getMovedPropertyName(), blobKey.getKeyString());
        }
      }
    }
  }

  public void postGet(Entity entity) {
    Preconditions.checkNotNull(entity, "Null entity");
    for (MovableProperty property : properties) {
      if (entity.hasProperty(property.getMovedPropertyName())) {
        Preconditions.checkArgument(!entity.hasProperty(property.getPropertyName()),
            "Entity %s already has property %s", entity, property);
        BlobKey blobKey = new BlobKey(
            (String) entity.getProperty(property.getMovedPropertyName()));
        byte[] bytes;
        try {
          bytes = slurp(blobKey);
        } catch (IOException e) {
          throw new RuntimeException(
              "Failed to fetch property " + property + " from blob " + blobKey + ": " + entity, e);
        } catch (ApiProxyException e) {
          // TODO(ohler): Change files API to make sure we get only IOExceptions.
          throw new RuntimeException(
              "Failed to fetch property " + property + " from blob " + blobKey + ": " + entity, e);
        }
        log.info("Fetched " + bytes.length + " bytes for " + property + " from blob " + blobKey);
        Object value;
        switch (property.getType()) {
          case BLOB:
            value = new Blob(bytes);
            break;
          case TEXT:
            value = new Text(decode(bytes));
            break;
          default:
            throw new RuntimeException("Unexpected type in " + property);
        }
        entity.removeProperty(property.getMovedPropertyName());
        entity.setProperty(property.getPropertyName(), value);
      }
    }
  }

  /**
   * To delete an entity with oversized properties, fetch the entity you want to
   * delete, delete it and commit the transaction, then pass the entity into
   * postDelete().
   */
  public void postDelete(Entity entity) {
    Preconditions.checkNotNull(entity, "Null entity");
    for (MovableProperty property : properties) {
      if (entity.hasProperty(property.getMovedPropertyName())) {
        BlobKey blobKey = new BlobKey(
            (String) entity.getProperty(property.getMovedPropertyName()));
        BlobstoreServiceFactory.getBlobstoreService().delete(blobKey);
        log.info("Deleted blob for " + property + ": " + blobKey);
        listener.blobDeleted(entity.getKey(), property, blobKey);
      }
    }
  }

}
