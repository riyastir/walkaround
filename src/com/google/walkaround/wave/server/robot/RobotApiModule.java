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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.wave.api.RobotSerializer;
import com.google.wave.api.data.converter.EventDataConverterModule;
import com.google.wave.api.robot.RobotConnection;

import org.waveprotocol.box.server.robots.RobotSerializerModule;
import org.waveprotocol.box.server.robots.passive.RobotConnector;

/**
 * Guice module for the RobotAPI.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class RobotApiModule extends AbstractModule {

  @Override
  protected void configure() {
    install(new EventDataConverterModule());
    install(new RobotSerializerModule());
  }

  @Provides
  @Inject
  @Singleton
  protected RobotConnector provideRobotConnector(
      RobotConnection connection, RobotSerializer serializer) {
    return new RobotConnector(connection, serializer);
  }

  @Provides
  @Singleton
  protected RobotConnection provideRobotConnection() {
    return new AppEngineRobotConnection();
  }
}
