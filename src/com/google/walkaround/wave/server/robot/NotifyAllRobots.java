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

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.inject.Inject;
import com.google.walkaround.slob.server.MutationLog;
import com.google.walkaround.slob.server.SlobFacilities;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.StateAndVersion;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.shared.Assert;
import com.google.walkaround.wave.server.GuiceSetup;
import com.google.walkaround.wave.server.conv.ConvStore;
import com.google.walkaround.wave.server.model.WaveObjectStoreModel.ReadableWaveletObject;

import org.waveprotocol.wave.model.wave.ParticipantId;

import java.util.List;
import java.util.logging.Logger;

/**
 * Notifies all robots on a conversational wavelet. Fans out to one task per
 * robot.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
class NotifyAllRobots implements DeferredTask {
  private static final long serialVersionUID = 557788136019705308L;
  private static final Logger log = Logger.getLogger(NotifyAllRobots.class.getName());

  static final String ROBOT_QUEUE_NAME = "robot-notifications";
  private static final Queue QUEUE = QueueFactory.getQueue(ROBOT_QUEUE_NAME);

  private static class Worker {
    @Inject CheckedDatastore datastore;
    @Inject @ConvStore SlobFacilities slobFacilities;

    private ReadableWaveletObject getWavelet(SlobId id, long expectedMinVersion)
        throws PermanentFailure, RetryableFailure {
      StateAndVersion raw;
      CheckedTransaction tx = datastore.beginTransaction();
      try {
        MutationLog l = slobFacilities.getMutationLogFactory().create(tx, id);
        raw = l.reconstruct(null);
      } finally {
        tx.rollback();
      }
      Assert.check(
          raw.getVersion() >= expectedMinVersion, "%s < %s", raw.getVersion(), expectedMinVersion);
      return (ReadableWaveletObject) raw.getState();
    }

    public void run(SlobId convSlobId, long newVersion) throws PermanentFailure, RetryableFailure {
      // TODO(ljv): Deal with the case where robots are removed from a wave.
      ReadableWaveletObject wavelet = getWavelet(convSlobId, newVersion);
      List<ParticipantId> robots = RobotIdHelper.getAllRobotIds(wavelet.getParticipants());

      // TODO(ljv): Shortcut for a wave with a single robot.
      for (ParticipantId robotId : robots) {
        DeferredTask notifyRobot = new NotifyRobot(convSlobId, newVersion, robotId);
        QUEUE.add(TaskOptions.Builder.withPayload(notifyRobot));
      }

      log.info("Scheduled robot notifications: " + robots);
    }
  }

  private final SlobId convSlobId;
  private final long newVersion;

  public NotifyAllRobots(SlobId convSlobId, long newVersion) {
    this.convSlobId = convSlobId;
    this.newVersion = newVersion;
  }

  @Override
  public void run() {
    try {
      GuiceSetup.getInjectorForTaskQueueTask()
          .getInstance(Worker.class).run(convSlobId, newVersion);
    } catch (PermanentFailure e) {
      throw new RuntimeException(e);
    } catch (RetryableFailure e) {
      // TODO: retry
      throw new RuntimeException(e);
    }
  }
}
