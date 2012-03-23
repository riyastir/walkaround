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

import org.waveprotocol.wave.model.wave.ParticipantId;

import java.util.List;

/**
 * Helper for identifying participant IDs that are robots.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class RobotIdHelper {

  private RobotIdHelper() {
  }

  /**
   * Returns true if the list of participants contains a robot.
   */
  public static boolean containsRobotId(List<ParticipantId> participants) {
    for (ParticipantId participant : participants) {
      if (isRobotId(participant)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if the particpant is a robot. This method will need to change
   * significantly if we allow robots from other domains. It works well for a
   * first-version where we support only AppEngine robots.
   */
  public static boolean isRobotId(ParticipantId participant) {
    return participant.getDomain().equals("appspot.com");
  }
}
