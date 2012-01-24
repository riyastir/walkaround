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

package com.google.walkaround.wave.server.conv;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.inject.Inject;
import com.google.walkaround.proto.ConvMetadata;
import com.google.walkaround.proto.gson.ConvMetadataGsonImpl;
import com.google.walkaround.slob.server.GsonProto;
import com.google.walkaround.slob.server.SlobFacilities;
import com.google.walkaround.slob.shared.MessageException;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.server.appengine.DatastoreUtil;

import java.util.logging.Logger;

/**
 * Allows access to the non-model {@link ConvMetadata} that is stored in the
 * same entity group as the slob data, but in an entity kind not under the slob
 * layer's control.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class ConvMetadataStore {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(ConvMetadataStore.class.getName());

  private static final String ENTITY_KIND = "ConvMetadata";
  private static final String ENTITY_KEY = "ConvMetadata";
  private static final String PROPERTY = "convMetadata";

  private final SlobFacilities convSlobFacilities;

  @Inject
  public ConvMetadataStore(@ConvStore SlobFacilities convSlobFacilities) {
    this.convSlobFacilities = convSlobFacilities;
  }

  private Key makeKey(SlobId convId) {
    return KeyFactory.createKey(convSlobFacilities.makeRootEntityKey(convId),
        ENTITY_KIND, ENTITY_KEY);
  }

  public ConvMetadataGsonImpl get(CheckedTransaction tx, SlobId convId)
      throws PermanentFailure, RetryableFailure {
    checkNotNull(tx, "Null tx");
    checkNotNull(convId, "Null convId");
    Entity entity = tx.get(makeKey(convId));
    if (entity == null) {
      log.info("No conv metadata found, using default");
      return new ConvMetadataGsonImpl();
    }
    String metadataString =
        DatastoreUtil.getExistingProperty(entity, PROPERTY, Text.class).getValue();
    log.info("Found conv metadata: " + metadataString);
    ConvMetadataGsonImpl metadata;
    try {
      metadata = GsonProto.fromGson(new ConvMetadataGsonImpl(), metadataString);
    } catch (MessageException e) {
      throw new RuntimeException("Failed to parse metadata: " + metadataString, e);
    }
    return metadata;
  }

  public void put(CheckedTransaction tx, SlobId convId, ConvMetadataGsonImpl metadata)
      throws PermanentFailure, RetryableFailure {
    checkNotNull(tx, "Null tx");
    checkNotNull(convId, "Null convId");
    checkNotNull(metadata, "Null metadata");
    String jsonString = GsonProto.toJson(metadata);
    log.info("Putting metadata: " + jsonString);
    Entity entity = new Entity(makeKey(convId));
    DatastoreUtil.setNonNullUnindexedProperty(entity, PROPERTY, new Text(jsonString));
    tx.put(entity);
  }

}
