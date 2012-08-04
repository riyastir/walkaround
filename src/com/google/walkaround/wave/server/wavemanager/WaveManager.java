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

package com.google.walkaround.wave.server.wavemanager;

import com.google.inject.Inject;
import com.google.walkaround.proto.ConvMetadata;
import com.google.walkaround.slob.server.MutationLog;
import com.google.walkaround.slob.server.SlobFacilities;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.StateAndVersion;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.wave.server.conv.ConvMetadataStore;
import com.google.walkaround.wave.server.conv.PermissionCache;
import com.google.walkaround.wave.server.conv.PermissionCache.Permissions;
import com.google.walkaround.wave.server.model.WaveObjectStoreModel.ReadableWaveletObject;

import org.waveprotocol.wave.model.wave.ParticipantId;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Extracts permissions from conv wavelets.
 *
 * @author ohler@google.com (Christian Ohler)
 */
// TODO(ohler): Rename to ConvPermissionSource and move to conv.
public class WaveManager implements PermissionCache.PermissionSource {

  private static final Logger log = Logger.getLogger(WaveManager.class.getName());

  private final CheckedDatastore datastore;
  private final ParticipantId participantId;
  private final SlobFacilities convSlobFacilities;
  private final ConvMetadataStore convMetadataStore;

  @Inject
  public WaveManager(CheckedDatastore datastore,
      ParticipantId participantId,
      SlobFacilities convSlobFacilities,
      ConvMetadataStore convMetadataStore) {
    this.datastore = datastore;
    this.participantId = participantId;
    this.convSlobFacilities = convSlobFacilities;
    this.convMetadataStore = convMetadataStore;
  }

  @Override
  public Permissions getPermissions(final SlobId slobId) throws IOException {
    try {
      return new RetryHelper().run(
          new RetryHelper.Body<Permissions>() {
            @Override public Permissions run() throws RetryableFailure, PermanentFailure {
              CheckedTransaction tx = datastore.beginTransaction();
              try {
                ConvMetadata metadata = convMetadataStore.get(tx, slobId);
                if (metadata.hasImportMetadata()
                    && !metadata.getImportMetadata().getImportFinished()) {
                  log.info(slobId + " still importing; " + participantId + " may not access");
                  return new Permissions(false, false);
                }
                // Can't use SlobStore here, that would be a cyclic dependency.
                MutationLog mutationLog = convSlobFacilities.getMutationLogFactory()
                    .create(tx, slobId);
                StateAndVersion stateAndVersion = mutationLog.reconstruct(null);
                if (stateAndVersion.getVersion() == 0) {
                  log.info(slobId + " does not exist; " + participantId + " may not access");
                  return new Permissions(false, false);
                }
                // TODO(ohler): introduce generics to avoid the cast.
                ReadableWaveletObject wavelet = (ReadableWaveletObject) stateAndVersion.getState();
                if (wavelet.getParticipants().contains(participantId)) {
                  log.info(slobId + " exists and " + participantId + " is on the participant list");
                  return new Permissions(true, true);
                } else {
                  log.info(slobId + " exists but "
                      + participantId + " is not on the participant list");
                  return new Permissions(false, false);
                }
              } finally {
                tx.close();
              }
            }
          });
    } catch (PermanentFailure e) {
      throw new IOException("Failed to get permissions for " + slobId);
    }
  }

}
