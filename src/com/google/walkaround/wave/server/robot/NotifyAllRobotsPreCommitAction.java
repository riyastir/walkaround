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

import java.util.List;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.walkaround.slob.server.PreCommitAction;
import com.google.walkaround.slob.shared.ChangeData;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.SlobModel.ReadableSlob;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.wave.server.model.WaveObjectStoreModel.ReadableWaveletObject;

/**
 * Enqueues a task queue task if the wavelet that is being updated contains a
 * robot as a participant.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class NotifyAllRobotsPreCommitAction implements PreCommitAction {

  private static final Queue QUEUE = QueueFactory.getQueue(NotifyAllRobots.ROBOT_QUEUE_NAME);

  @Override
  public void run(
      CheckedTransaction tx, SlobId objectId, List<ChangeData<String>> newDeltas,
      long resultingVersion, ReadableSlob resultingState)
      throws RetryableFailure, PermanentFailure {
    ReadableWaveletObject wavelet = (ReadableWaveletObject) resultingState;
    if (RobotIdHelper.containsRobotId(wavelet.getParticipants())) {
      DeferredTask task = new NotifyAllRobots(objectId, resultingVersion);
      tx.enqueueTask(QUEUE, TaskOptions.Builder.withPayload(task));
    }
  }
}
