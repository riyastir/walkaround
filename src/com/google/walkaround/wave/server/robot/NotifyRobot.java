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

import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.walkaround.slob.server.MutationLog;
import com.google.walkaround.slob.server.MutationLog.DeltaIterator;
import com.google.walkaround.slob.server.SlobFacilities;
import com.google.walkaround.slob.shared.MessageException;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.StateAndVersion;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.wave.server.GuiceSetup;
import com.google.walkaround.wave.server.conv.ConvStore;
import com.google.walkaround.wave.server.model.ServerMessageSerializer;
import com.google.walkaround.wave.server.robot.VersionDirectory.RobotNotified;
import com.google.walkaround.wave.shared.IdHack;
import com.google.walkaround.wave.shared.WaveSerializer;
import com.google.wave.api.Context;
import com.google.wave.api.OperationRequest;
import com.google.wave.api.ProtocolVersion;
import com.google.wave.api.data.converter.EventDataConverter;
import com.google.wave.api.data.converter.EventDataConverterManager;
import com.google.wave.api.event.Event;
import com.google.wave.api.event.EventType;
import com.google.wave.api.impl.EventMessageBundle;
import com.google.wave.api.robot.Capability;
import com.google.wave.api.robot.RobotName;

import com.googlecode.charts4j.collect.Maps;

import org.waveprotocol.box.common.DeltaSequence;
import org.waveprotocol.box.server.robots.passive.EventGenerator;
import org.waveprotocol.box.server.robots.passive.WaveletAndDeltas;
import org.waveprotocol.box.server.robots.util.ConversationUtil;
import org.waveprotocol.wave.model.operation.OperationException;
import org.waveprotocol.wave.model.operation.wave.TransformedWaveletDelta;
import org.waveprotocol.wave.model.operation.wave.WaveletOperation;
import org.waveprotocol.wave.model.operation.wave.WaveletOperationContext;
import org.waveprotocol.wave.model.util.Pair;
import org.waveprotocol.wave.model.version.HashedVersion;
import org.waveprotocol.wave.model.wave.ParticipantId;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Task that notifies a single robot of events on a single wavelet.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class NotifyRobot implements DeferredTask {
  private static final long serialVersionUID = 5;

  private static final Logger log = Logger.getLogger(NotifyRobot.class.getName());

  private static final WaveSerializer SERIALIZER =
      new WaveSerializer(new ServerMessageSerializer());

  private final SlobId convSlobId;
  private final long newVersion;
  private final ParticipantId robotId;

  public NotifyRobot(SlobId convSlobId, long newVersion, ParticipantId robotId) {
    this.convSlobId = convSlobId;
    this.newVersion = newVersion;
    this.robotId = robotId;
  }

  /**
   * Private class doing the actual work. This allows us to use Guice for
   * injection.
   */
  private static class Worker {
    @Inject
    CheckedDatastore datastore;
    @Inject
    @ConvStore
    SlobFacilities slobFacilities;
    @Inject
    VersionDirectory versionDirectory;
    @Inject
    EventDataConverterManager converterManager;
    @Inject
    RobotConnector connector;

    private WaveletAndDeltas getWaveletAndDeltas(SlobId id, long fromVersion)
        throws PermanentFailure, RetryableFailure {
      CheckedTransaction tx = datastore.beginTransaction();
      try {
        MutationLog l = slobFacilities.getMutationLogFactory().create(tx, id);
        StateAndVersion raw = l.reconstruct(null);
        DeltaIterator deltas = l.forwardHistory(
            fromVersion, null, FetchOptions.Builder.withDefaults());
        List<TransformedWaveletDelta> accu = Lists.newArrayList();
        long version = fromVersion;
        while (deltas.hasNext()) {
          try {
            accu.add(
                convertDelta(version, SERIALIZER.deserializeDelta(deltas.next().getPayload())));
          } catch (MessageException e) {
            throw new RuntimeException(e);
          }
          version++;
        }
        try {
          return WaveletAndDeltas.create(
              SERIALIZER.deserializeWavelet(
                  IdHack.convWaveletNameFromConvObjectId(id), raw.getState().snapshot()),
              DeltaSequence.of(accu));
        } catch (MessageException e) {
          throw new RuntimeException(e);
        } catch (OperationException e) {
          throw new RuntimeException(e);
        }
      } finally {
        tx.rollback();
      }
    }

    private TransformedWaveletDelta convertDelta(long version, WaveletOperation op) {
      WaveletOperationContext context = op.getContext();
      ParticipantId creator = context.getCreator();
      HashedVersion hashedVersion = context.getHashedVersion();
      return TransformedWaveletDelta.cloneOperations(
          context.getCreator(), HashedVersion.unsigned(version + context.getVersionIncrement()),
          context.getTimestamp(), ImmutableList.of(op));
    }

    /**
     * Executes the passive API call for a single robot.
     */
    public void run(SlobId convSlobId, long newVersion, ParticipantId robotId)
        throws PermanentFailure, RetryableFailure, IOException {
      RobotNotified entry = versionDirectory.getWithoutTx(Pair.of(convSlobId, robotId));
      if (entry == null) {
        // TODO: figure out what version the robot was added, and notify from
        // there.
        entry = new RobotNotified(convSlobId, robotId, newVersion - 1);
      }
      long lastSeen = entry.getVersion();
      if (lastSeen >= newVersion) {
        return;
      }
      WaveletAndDeltas waveletAndDeltas = getWaveletAndDeltas(convSlobId, lastSeen);

      // 1. Generate the events.
      EventMessageBundle bundle = generatEventMessageBundle(robotId, waveletAndDeltas);

      List<Event> events = bundle.getEvents();
      log.info(events.size() + " events generated");
      for (Event event : events) {
        log.info("Type of event generated: " + event.getType());
      }

      // 2. Notify the robot of the event.
      List<OperationRequest> requests =
          connector.sendMessageBundle(bundle, robotId, ProtocolVersion.DEFAULT);

      log.info(requests.size() + " requests returned");
      for (OperationRequest request : requests) {
        log.info(request.toString());
      }

      // 3. Update the last version the robot has seen.
      // TODO(ljv): Deal with the case where robots are removed from a wave?
      log.info("Robot " + robotId + ": last seen " + lastSeen + ", now at "
          + waveletAndDeltas.getVersionAfterDeltas());
      versionDirectory.putWithoutTx(
          new RobotNotified(convSlobId, robotId, waveletAndDeltas.getVersionAfterDeltas()
              .getVersion()));

      // 4. Apply the changes the robot wants applied.
      // TODO(ljv): Apply the results.


    }

    private EventMessageBundle generatEventMessageBundle(
        ParticipantId robotId, WaveletAndDeltas waveletAndDeltas) {
      ConversationUtil conversationUtil = new ConversationUtil(new IdHack.StubIdGenerator());
      EventGenerator generator =
          new EventGenerator(RobotName.fromAddress(robotId.getAddress()), conversationUtil);

      // TODO(ljv): Cache this?
      // RobotCapabilities capabilities;
      // try {
      // // TODO(ljv): Todo reenable when JDOM classes are included in build
      // //capabilities = connector.fetchCapabilities(robotId, "");
      // } catch (CapabilityFetchException e) {
      // log.log(Level.WARNING, "Error occured fetching capabilities", e);
      // return new EventMessageBundle(robotId.getAddress(), "");
      // }

      // Some default capabilities for now that cover an echo bot :).
      Map<EventType, Capability> capabilities = Maps.newHashMap();
      List<Context> context = Lists.newArrayList(Context.SIBLINGS, Context.SELF);
      capabilities.put(
          EventType.WAVELET_BLIP_CREATED, new Capability(EventType.WAVELET_BLIP_CREATED, context));
      capabilities.put(
          EventType.ANNOTATED_TEXT_CHANGED,
          new Capability(EventType.ANNOTATED_TEXT_CHANGED, context));
      capabilities.put(
          EventType.DOCUMENT_CHANGED, new Capability(EventType.DOCUMENT_CHANGED, context));

      // TODO(ljv): Support different versions
      EventDataConverter converter =
          converterManager.getEventDataConverter(ProtocolVersion.DEFAULT);
      return generator.generateEvents(waveletAndDeltas, capabilities, converter);
    }
  }

  @Override
  public void run() {
    try {
      GuiceSetup.getInjectorForTaskQueueTask().createChildInjector(new RobotApiModule())
          .getInstance(Worker.class).run(convSlobId, newVersion, robotId);
    } catch (PermanentFailure e) {
      throw new RuntimeException(e);
    } catch (RetryableFailure e) {
      // TODO: retry
      throw new RuntimeException(e);
    } catch (IOException e) {
      // TODO: retry
      throw new RuntimeException(e);
    }
  }

}
