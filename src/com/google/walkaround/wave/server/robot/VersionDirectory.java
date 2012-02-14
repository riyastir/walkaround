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

package com.google.walkaround.wave.server.robot;

import com.google.appengine.api.datastore.Entity;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.util.server.appengine.AbstractDirectory;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.DatastoreUtil;
import com.google.walkaround.util.shared.Assert;
import com.google.walkaround.wave.server.robot.VersionDirectory.RobotNotified;

import org.waveprotocol.wave.model.util.Pair;
import org.waveprotocol.wave.model.wave.ParticipantId;

/**
 * A mapping from <SlobId, RobotId> to version number that the robot has last
 * seen on the conversational wavelet.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class VersionDirectory extends AbstractDirectory<RobotNotified, Pair<SlobId, ParticipantId>> {

  private static final String VERSION_PROPERTY = "Version";

  public static class RobotNotified {
    private final Pair<SlobId, ParticipantId> id;
    private final long version;

    RobotNotified(SlobId slobId, ParticipantId robotId, long version) {
      Preconditions.checkNotNull(slobId);
      Preconditions.checkNotNull(robotId);

      this.id = Pair.of(slobId, robotId);
      this.version = version;
    }

    public long getVersion() {
      return version;
    }
  }

  @Inject
  public VersionDirectory(CheckedDatastore datastore) {
    super(datastore, "RobotNotified");
  }

  @Override
  protected Pair<SlobId, ParticipantId> getId(RobotNotified e) {
    return e.id;
  }

  @Override
  protected RobotNotified parse(Entity e) {
    String[] parts = e.getKey().getName().split(" ", -1);
    Assert.check(parts.length == 2, "Wrong number of spaces in key: %s", e);

    SlobId slobId = new SlobId(parts[0]);
    ParticipantId robotId = ParticipantId.ofUnsafe(parts[1]);

    long version = DatastoreUtil.getExistingProperty(e, VERSION_PROPERTY, Long.class);
    return new RobotNotified(slobId, robotId, version);
  }

  @Override
  protected void populateEntity(RobotNotified in, Entity out) {
    DatastoreUtil.setNonNullUnindexedProperty(out, VERSION_PROPERTY, in.version);
  }

  @Override
  protected String serializeId(Pair<SlobId, ParticipantId> id) {
    Assert.check(!id.getFirst().getId().contains(" "), "SlobId contains space: %s", id);
    Assert.check(!id.getSecond().getAddress().contains(" "), "RobotId contains space: %s", id);

    return id.getFirst().getId() + " " + id.getSecond().getAddress();
  }
}
