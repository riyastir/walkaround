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
import com.google.walkaround.slob.shared.SlobId;

import java.util.logging.Logger;

/**
 * Task that fans out to notify Robots of an update to a conversational wavelet
 * of which they are a participant.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class NotifyAllRobots implements DeferredTask {
  private static final long serialVersionUID = 1L;
  private static final Logger log = Logger.getLogger(NotifyAllRobots.class.getName());

  public static final String ROBOT_QUEUE_NAME = "robot-notifications";

  @SuppressWarnings("unused")
  private final SlobId waveletId;
  @SuppressWarnings("unused")
  private final long newVersion;

  public NotifyAllRobots(SlobId waveletId, long newVersion) {
    this.waveletId = waveletId;
    this.newVersion = newVersion;
  }

  @Override
  public void run() {
    // TODO(ljv): Implement the process of notifying robots and applying their
    // responses.
    log.info("The task that should notify robots has run!");
  }
}
