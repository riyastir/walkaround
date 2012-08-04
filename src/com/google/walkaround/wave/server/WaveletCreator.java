/*
 * Copyright 2011 Google Inc. All Rights Reserved.
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

package com.google.walkaround.wave.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.walkaround.proto.ObsoleteWaveletMetadata;
import com.google.walkaround.proto.gson.ConvMetadataGsonImpl;
import com.google.walkaround.proto.gson.ObsoleteUdwMetadataGsonImpl;
import com.google.walkaround.proto.gson.ObsoleteWaveletMetadataGsonImpl;
import com.google.walkaround.slob.server.AccessDeniedException;
import com.google.walkaround.slob.server.GsonProto;
import com.google.walkaround.slob.server.SlobAlreadyExistsException;
import com.google.walkaround.slob.server.SlobStore;
import com.google.walkaround.slob.shared.ChangeData;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.shared.RandomBase64Generator;
import com.google.walkaround.wave.server.WaveletDirectory.ObjectIdAlreadyKnown;
import com.google.walkaround.wave.server.auth.StableUserId;
import com.google.walkaround.wave.server.conv.ConvMetadataStore;
import com.google.walkaround.wave.server.conv.ConvStore;
import com.google.walkaround.wave.server.model.ClientIdGenerator;
import com.google.walkaround.wave.server.model.InitialOps;
import com.google.walkaround.wave.server.model.ServerMessageSerializer;
import com.google.walkaround.wave.server.udw.UdwStore;
import com.google.walkaround.wave.server.udw.UserDataWaveletDirectory;
import com.google.walkaround.wave.shared.WaveSerializer;

import org.waveprotocol.wave.model.operation.wave.WaveletOperation;
import org.waveprotocol.wave.model.wave.ParticipantId;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Creates wavelets.
 *
 * @author danilatos@google.com (Daniel Danilatos)
 * @author ohler@google.com (Christian Ohler)
 */
public class WaveletCreator {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(WaveletCreator.class.getName());

  private static final WaveSerializer SERIALIZER =
      new WaveSerializer(new ServerMessageSerializer());

  private final WaveletDirectory waveletDirectory;
  private final UserDataWaveletDirectory udwDirectory;
  private final ParticipantId participantId;
  private final StableUserId userId;
  private final SlobStore convStore;
  private final SlobStore udwStore;
  private final RandomBase64Generator random64;
  private final CheckedDatastore datastore;
  private final ConvMetadataStore convMetadataStore;
  private final ClientIdGenerator clientIdGenerator;

  @Inject
  public WaveletCreator(
      WaveletDirectory waveletDirectory,
      UserDataWaveletDirectory udwDirectory,
      ParticipantId participantId,
      StableUserId userId,
      @ConvStore SlobStore convStore,
      @UdwStore SlobStore udwStore,
      RandomBase64Generator random64,
      CheckedDatastore datastore,
      ConvMetadataStore convMetadataStore,
      ClientIdGenerator clientIdGenerator) {
    this.waveletDirectory = waveletDirectory;
    this.udwDirectory = udwDirectory;
    this.participantId = participantId;
    this.userId = userId;
    this.convStore = convStore;
    this.udwStore = udwStore;
    this.random64 = random64;
    this.datastore = datastore;
    this.convMetadataStore = convMetadataStore;
    this.clientIdGenerator = clientIdGenerator;
  }

  private WaveletMapping registerWavelet(SlobId objectId) throws IOException {
    WaveletMapping mapping = new WaveletMapping(objectId);
    log.info("Registering " + mapping);
    try {
      waveletDirectory.register(mapping);
    } catch (ObjectIdAlreadyKnown e) {
      throw new RuntimeException("Freshly-created object already known", e);
    }
    log.info("Registered " + mapping);
    return mapping;
  }

  private String makeObsoleteUdwMetadata(SlobId convId) {
    ObsoleteUdwMetadataGsonImpl udwMeta = new ObsoleteUdwMetadataGsonImpl();
    udwMeta.setAssociatedConvId(convId.getId());
    udwMeta.setOwner(userId.getId());
    ObsoleteWaveletMetadataGsonImpl meta = new ObsoleteWaveletMetadataGsonImpl();
    meta.setType(ObsoleteWaveletMetadata.Type.UDW);
    meta.setUdwMetadata(udwMeta);
    return GsonProto.toJson(meta);
  }

  private String makeObsoleteConvMetadata() {
    ObsoleteWaveletMetadataGsonImpl meta = new ObsoleteWaveletMetadataGsonImpl();
    meta.setType(ObsoleteWaveletMetadata.Type.CONV);
    return GsonProto.toJson(meta);
  }

  private List<ChangeData<String>> serializeChanges(List<WaveletOperation> ops) {
    ImmutableList.Builder<ChangeData<String>> out = ImmutableList.builder();
    for (String delta : SERIALIZER.serializeDeltas(ops)) {
      out.add(new ChangeData<String>(clientIdGenerator.getIdForCreation(), delta));
    }
    return out.build();
  }

  private WaveletMapping createUdw(SlobId convId) throws IOException {
    long creationTime = System.currentTimeMillis();
    List<WaveletOperation> history = InitialOps.userDataWaveletOps(participantId, creationTime);
    SlobId objectId = newUdwWithGeneratedId(makeObsoleteUdwMetadata(convId),
        serializeChanges(history));
    return registerWavelet(objectId);
  }

  public SlobId getOrCreateUdw(SlobId convId) throws IOException {
    SlobId existing = udwDirectory.getUdwId(convId, userId);
    if (existing != null) {
      return existing;
    } else {
      SlobId newUdwId = createUdw(convId)
          .getObjectId();
      SlobId existingUdwId = udwDirectory.getOrAdd(convId, userId, newUdwId);
      if (existingUdwId == null) {
        return newUdwId;
      } else {
        log.log(Level.WARNING, "Multiple concurrent UDW creations for "
            + userId + " on " + convId + ": " + existingUdwId + ", " + newUdwId);
        return existingUdwId;
      }
    }
  }

  public SlobId newConvWithGeneratedId(
      @Nullable List<WaveletOperation> historyOps,
      @Nullable final ConvMetadataGsonImpl convMetadata, final boolean inhibitPreAndPostCommit)
      throws IOException {
    final List<ChangeData<String>> history;
    if (historyOps != null) {
      history = serializeChanges(historyOps);
    } else {
      history = serializeChanges(
          InitialOps.conversationWaveletOps(participantId, System.currentTimeMillis(), random64));
    }
    SlobId newId;
    try {
      newId = new RetryHelper().run(
          new RetryHelper.Body<SlobId>() {
            @Override public SlobId run() throws RetryableFailure, PermanentFailure {
              SlobId convId = getRandomObjectId();
              CheckedTransaction tx = datastore.beginTransaction();
              try {
                convStore.newObject(tx, convId, makeObsoleteConvMetadata(), history,
                    inhibitPreAndPostCommit);
                convMetadataStore.put(tx, convId,
                    convMetadata != null ? convMetadata : new ConvMetadataGsonImpl());
                tx.commit();
              } catch (SlobAlreadyExistsException e) {
                throw new RetryableFailure("Slob id collision, retrying: " + convId, e);
              } catch (AccessDeniedException e) {
                throw new RuntimeException(
                    "Unexpected AccessDeniedException creating conv " + convId, e);
              } finally {
                tx.close();
              }
              return convId;
            }
          });
    } catch (PermanentFailure e) {
      throw new IOException("PermanentFailure creating conv", e);
    }
    registerWavelet(newId);
    return newId;
  }

  private SlobId getRandomObjectId() {
    // 96 random bits.  (6 bits per base64 character.)  TODO(ohler): Justify the
    // number 96.
    return new SlobId(random64.next(96 / 6));
  }

  private SlobId newUdwWithGeneratedId(final String metadata,
      final List<ChangeData<String>> initialHistory) throws IOException {
    try {
      return new RetryHelper().run(
          new RetryHelper.Body<SlobId>() {
            @Override public SlobId run() throws RetryableFailure, PermanentFailure {
              SlobId udwId = getRandomObjectId();
              CheckedTransaction tx = datastore.beginTransaction();
              try {
                udwStore.newObject(tx, udwId, metadata, initialHistory, false);
                tx.commit();
              } catch (SlobAlreadyExistsException e) {
                throw new RetryableFailure("Slob id collision, retrying: " + udwId, e);
              } catch (AccessDeniedException e) {
                throw new RuntimeException(
                    "Unexpected AccessDeniedException creating udw " + udwId, e);
              } finally {
                tx.close();
              }
              return udwId;
            }
          });
    } catch (PermanentFailure e) {
      throw new IOException("PermanentFailure creating udw", e);
    }
  }

}
