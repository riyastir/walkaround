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

package com.google.walkaround.wave.server.googleimport;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.Entity;
import com.google.common.base.Objects;
import com.google.inject.Inject;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.AbstractDirectory;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.server.appengine.DatastoreUtil;
import com.google.walkaround.util.shared.Assert;
import com.google.walkaround.wave.server.gxp.SourceInstance;

import org.waveprotocol.wave.model.id.WaveId;
import org.waveprotocol.wave.model.id.WaveletId;
import org.waveprotocol.wave.model.id.WaveletName;
import org.waveprotocol.wave.model.util.Pair;

import java.io.IOException;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * Tracks information about shared imported wavelets.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class SharedImportTable {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(SharedImportTable.class.getName());

  private static class Entry {
    // By design, wave ids should be unique across all instances, but it's easy
    // not to rely on this, so let's not.
    private final SourceInstance sourceInstance;
    private final WaveletName waveletName;
    private final SlobId slobId;

    public Entry(SourceInstance sourceInstance,
        WaveletName waveletName,
        SlobId slobId) {
      this.sourceInstance = checkNotNull(sourceInstance, "Null sourceInstance");
      this.waveletName = checkNotNull(waveletName, "Null waveletName");
      this.slobId = checkNotNull(slobId, "Null slobId");
    }

    public SourceInstance getSourceInstance() {
      return sourceInstance;
    }

    public WaveletName getWaveletName() {
      return waveletName;
    }

    public SlobId getSlobId() {
      return slobId;
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "("
          + sourceInstance + ", "
          + waveletName + ", "
          + slobId
          + ")";
    }

    @Override public final boolean equals(Object o) {
      if (o == this) { return true; }
      if (!(o instanceof Entry)) { return false; }
      Entry other = (Entry) o;
      return Objects.equal(sourceInstance, other.sourceInstance)
          && Objects.equal(waveletName, other.waveletName)
          && Objects.equal(slobId, other.slobId);
    }

    @Override public final int hashCode() {
      return Objects.hashCode(sourceInstance, waveletName, slobId);
    }
  }

  private static final String ROOT_ENTITY_KIND = "SharedImportedWavelet";
  private static final String SLOB_ID_PROPERTY = "slobId";

  private static class Directory
      extends AbstractDirectory<Entry, Pair<SourceInstance, WaveletName>> {
    private final SourceInstance.Factory sourceInstanceFactory;

    @Inject Directory(CheckedDatastore datastore,
        SourceInstance.Factory sourceInstanceFactory) {
      super(datastore, ROOT_ENTITY_KIND);
      this.sourceInstanceFactory = sourceInstanceFactory;
    }

    private Pair<SourceInstance, WaveletName> parseId(String in) {
      String[] components = in.split(" ", -1);
      Assert.check(components.length == 3, "Wrong number of spaces: %s", in);
      return Pair.of(
        sourceInstanceFactory.parseUnchecked(components[0]),
        WaveletName.of(WaveId.deserialise(components[1]),
            WaveletId.deserialise(components[2])));
    }

    @Override protected String serializeId(Pair<SourceInstance, WaveletName> id) {
      String out = id.getFirst().serialize()
          + " " + id.getSecond().waveId.serialise() + " " + id.getSecond().waveletId.serialise();
      Assert.check(id.equals(parseId(out)), "Failed to serialize id: %s", id);
      return out;
    }

    @Override protected Pair<SourceInstance, WaveletName> getId(Entry e) {
      return Pair.of(e.getSourceInstance(), e.getWaveletName());
    }

    @Override protected void populateEntity(Entry in, Entity out) {
      DatastoreUtil.setNonNullIndexedProperty(out, SLOB_ID_PROPERTY, in.getSlobId().getId());
    }

    @Override protected Entry parse(Entity in) {
      Pair<SourceInstance, WaveletName> id = parseId(in.getKey().getName());
      return new Entry(id.getFirst(), id.getSecond(),
          new SlobId(DatastoreUtil.getExistingProperty(in, SLOB_ID_PROPERTY, String.class)));
    }
  }


  private final Directory directory;

  @Inject
  public SharedImportTable(Directory directory) {
    this.directory = directory;
  }

  @Nullable private SlobId nullableEntryToSlobId(@Nullable Entry entry) {
    return entry == null ? null : entry.slobId;
  }

  @Nullable public SlobId lookup(
      CheckedTransaction tx, SourceInstance instance, WaveletName waveletName)
      throws PermanentFailure, RetryableFailure {
    return nullableEntryToSlobId(directory.get(tx, Pair.of(instance, waveletName)));
  }

  @Nullable public SlobId lookupWithoutTx(SourceInstance instance, WaveletName waveletName)
      throws IOException {
    return nullableEntryToSlobId(directory.getWithoutTx(Pair.of(instance, waveletName)));
  }

  @Nullable public void put(CheckedTransaction tx, SourceInstance instance,
      WaveletName waveletName, SlobId slobId) throws PermanentFailure, RetryableFailure {
    directory.put(tx, new Entry(instance, waveletName, slobId));
  }
}
