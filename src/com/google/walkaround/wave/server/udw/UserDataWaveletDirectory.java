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

package com.google.walkaround.wave.server.udw;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.repackaged.com.google.common.collect.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.AbstractDirectory;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.DatastoreUtil;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedIterator;
import com.google.walkaround.util.server.appengine.DatastoreUtil.InvalidPropertyException;
import com.google.walkaround.wave.server.auth.StableUserId;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author danilatos@google.com (Daniel Danilatos)
 * @author ohler@google.com (Christian Ohler)
 */
public class UserDataWaveletDirectory {
  private static final Logger log = Logger.getLogger(UserDataWaveletDirectory.class.getName());

  /** The first possible user id in the sort order */
  private static final StableUserId FIRST = new StableUserId("");
  private static final String ENTITY_KIND = "UdwDirectoryEntry3";

  public static class Key {
    private final SlobId convObjectId;
    private final StableUserId userId;

    private Key(SlobId convObjectId, StableUserId userId) {
      Preconditions.checkNotNull(convObjectId, "Null convObjectId");
      Preconditions.checkNotNull(userId, "Null userId");
      Preconditions.checkArgument(!convObjectId.getId().contains(" "),
          "Invalid convObjectId: %s", convObjectId);
      Preconditions.checkArgument(!userId.getId().contains(" "),
          "Invalid userId: %s", userId);
      this.convObjectId = convObjectId;
      this.userId = userId;
    }

    public SlobId getConvObjectId() {
      return convObjectId;
    }

    public StableUserId getUserId() {
      return userId;
    }

    @Override public String toString() {
      return "Key(" + convObjectId + ", " + userId + ")";
    }

    @Override public boolean equals(Object o) {
      if (o == this) { return true; }
      if (o == null) { return false; }
      if (!(o.getClass() == Key.class)) { return false; }
      Key other = (Key) o;
      return convObjectId.equals(other.convObjectId)
          && userId.equals(other.userId);
    }

    @Override public int hashCode() {
      return Objects.hashCode(Key.class, convObjectId, userId);
    }
  }

  public static class ConvUdwMapping {
    private final Key key;
    private final SlobId udwId;

    private ConvUdwMapping(Key key, SlobId udwId) {
      Preconditions.checkNotNull(key, "Null key");
      Preconditions.checkNotNull(udwId, "Null udwId");
      this.key = key;
      this.udwId = udwId;
    }

    public Key getKey() {
      return key;
    }

    public SlobId getUdwId() {
      return udwId;
    }

    @Override public String toString() {
      return "Entry(" + key + ", " + udwId + ")";
    }

    @Override public boolean equals(Object o) {
      if (o == this) { return true; }
      if (o == null) { return false; }
      if (!(o.getClass() == ConvUdwMapping.class)) { return false; }
      ConvUdwMapping other = (ConvUdwMapping) o;
      return key.equals(other.key)
          && udwId.equals(other.udwId);
    }

    @Override public int hashCode() {
      return Objects.hashCode(ConvUdwMapping.class, key, udwId);
    }
  }

  @VisibleForTesting
  static class Directory extends AbstractDirectory<ConvUdwMapping, Key> {
    private static final String UDW_ID_PROPERTY = "UdwId";

    Directory(CheckedDatastore datastore) {
      // Increased to 3 because the metadata format has changed.
      super(datastore, ENTITY_KIND);
    }

    @Override
    protected String serializeId(Key key) {
      return key.getConvObjectId().getId() + " " + key.getUserId().getId();
    }

    @Override
    protected Key getId(ConvUdwMapping entry) {
      return entry.getKey();
    }

    @Override
    protected void populateEntity(ConvUdwMapping entry, Entity out) {
      DatastoreUtil.setNonNullIndexedProperty(out, UDW_ID_PROPERTY, entry.getUdwId().getId());
    }

    private SlobId parseObjectId(Entity e, String propertyName, String objectIdStr) {
      return new SlobId(objectIdStr);
    }

    @Override
    protected ConvUdwMapping parse(Entity e) throws InvalidPropertyException {
      String key = e.getKey().getName();
      int space = key.indexOf(' ');
      if (space == -1 || space != key.lastIndexOf(' ')) {
        throw new InvalidPropertyException(e, "key");
      }
      SlobId convObjectId = parseObjectId(e, "key", key.substring(0, space));
      StableUserId userId = new StableUserId(key.substring(space + 1));
      SlobId udwId = parseObjectId(e, UDW_ID_PROPERTY,
          DatastoreUtil.getExistingProperty(e, UDW_ID_PROPERTY, String.class));
      return new ConvUdwMapping(new Key(convObjectId, userId), udwId);
    }
  }

  private final CheckedDatastore datastore;
  private final Directory directory;

  @Inject
  public UserDataWaveletDirectory(CheckedDatastore datastore) {
    this.datastore = datastore;
    this.directory = new Directory(datastore);
  }

  @Nullable public SlobId getUdwId(SlobId convObjectId, StableUserId userId) throws IOException {
    Preconditions.checkNotNull(convObjectId, "Null convObjectId");
    Preconditions.checkNotNull(userId, "Null userId");
    ConvUdwMapping e = directory.getWithoutTx(new Key(convObjectId, userId));
    return e == null ? null : e.getUdwId();
  }

  // HACK(danilatos): The throws declaration is inconsistent with the other methods,
  // but works better for the current single existing use of this method.
  public List<ConvUdwMapping> getAllUdwIds(SlobId convObjectId)
      throws PermanentFailure, RetryableFailure {
    Query q = new Query(ENTITY_KIND)
      .setFilter(FilterOperator.GREATER_THAN_OR_EQUAL.of(Entity.KEY_RESERVED_PROPERTY,
          directory.makeKey(new Key(convObjectId, FIRST))))
      .addSort(Entity.KEY_RESERVED_PROPERTY, SortDirection.ASCENDING);

    List<ConvUdwMapping> entries = Lists.newArrayList();

    CheckedIterator it = datastore.prepareNontransactionalQuery(q)
        .asIterator(FetchOptions.Builder.withDefaults());
    while (it.hasNext()) {
      ConvUdwMapping entry = directory.parse(it.next());
      if (!entry.getKey().getConvObjectId().equals(convObjectId)) {
        break;
      }
      entries.add(entry);
    }
    log.info("Found " + entries.size() + " user data wavelets for " + convObjectId);
    return entries;
  }

  public SlobId getOrAdd(SlobId convObjectId, StableUserId userId, SlobId udwId)
      throws IOException {
    Preconditions.checkNotNull(convObjectId, "Null convObjectId");
    Preconditions.checkNotNull(userId, "Null userId");
    Preconditions.checkNotNull(udwId, "Null udwId");
    ConvUdwMapping newEntry = new ConvUdwMapping(new Key(convObjectId, userId), udwId);
    ConvUdwMapping existingEntry = directory.getOrAdd(newEntry);
    if (existingEntry == null) {
      return null;
    } else {
      return existingEntry.getUdwId();
    }
  }

}
