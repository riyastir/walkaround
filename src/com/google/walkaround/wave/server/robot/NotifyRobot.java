/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.walkaround.wave.server.robot;

import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.walkaround.proto.ObjectSessionProto;
import com.google.walkaround.proto.ServerMutateRequest;
import com.google.walkaround.proto.gson.ObjectSessionProtoGsonImpl;
import com.google.walkaround.proto.gson.ServerMutateRequestGsonImpl;
import com.google.walkaround.slob.server.AccessDeniedException;
import com.google.walkaround.slob.server.MutationLog;
import com.google.walkaround.slob.server.MutationLog.DeltaIterator;
import com.google.walkaround.slob.server.SlobFacilities;
import com.google.walkaround.slob.server.SlobNotFoundException;
import com.google.walkaround.slob.server.SlobStoreSelector;
import com.google.walkaround.slob.shared.MessageException;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.StateAndVersion;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.shared.RandomBase64Generator;
import com.google.walkaround.wave.server.GuiceSetup;
import com.google.walkaround.wave.server.conv.ConvStore;
import com.google.walkaround.wave.server.model.ClientIdGenerator;
import com.google.walkaround.wave.server.model.ServerMessageSerializer;
import com.google.walkaround.wave.server.robot.VersionDirectory.RobotNotified;
import com.google.walkaround.wave.shared.IdHack;
import com.google.walkaround.wave.shared.WaveSerializer;
import com.google.wave.api.OperationRequest;
import com.google.wave.api.ProtocolVersion;
import com.google.wave.api.data.converter.EventDataConverter;
import com.google.wave.api.data.converter.EventDataConverterManager;
import com.google.wave.api.event.Event;
import com.google.wave.api.impl.EventMessageBundle;
import com.google.wave.api.robot.CapabilityFetchException;
import com.google.wave.api.robot.RobotName;

import org.waveprotocol.box.common.DeltaSequence;
import org.waveprotocol.box.server.robots.OperationServiceRegistry;
import org.waveprotocol.box.server.robots.RobotCapabilities;
import org.waveprotocol.box.server.robots.RobotWaveletData;
import org.waveprotocol.box.server.robots.passive.EventGenerator;
import org.waveprotocol.box.server.robots.passive.OperationServiceRegistryImpl;
import org.waveprotocol.box.server.robots.passive.WaveletAndDeltas;
import org.waveprotocol.box.server.robots.util.ConversationUtil;
import org.waveprotocol.box.server.robots.util.OperationUtil;
import org.waveprotocol.wave.model.id.IdConstants;
import org.waveprotocol.wave.model.id.SimplePrefixEscaper;
import org.waveprotocol.wave.model.id.WaveletName;
import org.waveprotocol.wave.model.operation.OperationException;
import org.waveprotocol.wave.model.operation.wave.TransformedWaveletDelta;
import org.waveprotocol.wave.model.operation.wave.WaveletDelta;
import org.waveprotocol.wave.model.operation.wave.WaveletOperation;
import org.waveprotocol.wave.model.operation.wave.WaveletOperationContext;
import org.waveprotocol.wave.model.util.Pair;
import org.waveprotocol.wave.model.version.HashedVersion;
import org.waveprotocol.wave.model.wave.ParticipantId;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Notifies a single robot of events on a single wavelet.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
class NotifyRobot implements DeferredTask {
  private static final long serialVersionUID = 635120842468246509L;

  private static final Logger log = Logger.getLogger(NotifyRobot.class.getName());

  private static final WaveSerializer SERIALIZER =
      new WaveSerializer(new ServerMessageSerializer());

  // TODO(ljv): Implement the notify operation.
  private static final OperationServiceRegistry PASSIVE_API =
      new OperationServiceRegistryImpl(null);

  private final SlobId convSlobId;
  private final long newVersion;
  private final ParticipantId robotId;

  public NotifyRobot(SlobId convSlobId, long newVersion, ParticipantId robotId) {
    this.convSlobId = convSlobId;
    this.newVersion = newVersion;
    this.robotId = robotId;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + convSlobId + ", "
        + newVersion + ", "
        + robotId + ")";
  }

  /**
   * Private class doing the actual work. This allows us to use Guice for
   * injection.
   */
  private static class Worker {
    @Inject CheckedDatastore datastore;
    @Inject @ConvStore SlobFacilities convFacilities;
    @Inject VersionDirectory versionDirectory;
    @Inject EventDataConverterManager converterManager;
    @Inject RobotConnector connector;
    @Inject SlobStoreSelector storeSelector;
    @Inject RandomBase64Generator random64;
    @Inject ClientIdGenerator clientIdGenerator;

    private WaveletAndDeltas getWaveletAndDeltas(SlobId id, long fromVersion)
        throws PermanentFailure, RetryableFailure {
      CheckedTransaction tx = datastore.beginTransaction();
      try {
        MutationLog l = convFacilities.getMutationLogFactory().create(tx, id);
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
          throw new RuntimeException("MessageException creating WaveletAndDeltas for " + id
              + "; fromVersion=" + fromVersion + ", version=" + version, e);
        } catch (OperationException e) {
          throw new RuntimeException("OperationException creating WaveletAndDeltas for " + id
              + "; fromVersion=" + fromVersion + ", version=" + version, e);
        }
      } finally {
        tx.rollback();
      }
    }

    private TransformedWaveletDelta convertDelta(long version, WaveletOperation op) {
      WaveletOperationContext context = op.getContext();
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
      EventMessageBundle bundle = generateEventMessageBundle(robotId, waveletAndDeltas);
      List<Event> events = bundle.getEvents();
      log.info(events.size() + " events generated");
      for (Event event : events) {
        log.info("Type of event generated: " + event.getType());
      }

      if (bundle.getEvents().isEmpty()) {
        // No need to notify the robot.
        updateRobotHasBeenNotified(convSlobId, robotId, lastSeen, waveletAndDeltas);
        return;
      }

      // 2. Notify the robot of the event.
      List<OperationRequest> operations =
          connector.sendMessageBundle(bundle, robotId, ProtocolVersion.DEFAULT);
      log.info(operations.size() + " operation requests returned");
      for (OperationRequest request : operations) {
        log.info(request.toString());
      }

      // 3. Update the last version the robot has seen.
      // TODO(ljv): Deal with the case where robots are removed from a wave?
      // TODO: Do this after applying the changes?
      updateRobotHasBeenNotified(convSlobId, robotId, lastSeen, waveletAndDeltas);

      // 4. Apply the changes the robot wants applied.
      // The robots we support should be sending us their version in their first
      // operation
      ProtocolVersion protocolVersion = OperationUtil.getProtocolVersion(operations);

      ConversationUtil conversationUtil = new ConversationUtil(
          new IdHack.StubIdGenerator() {
            @Override public String newBlipId() {
              return SimplePrefixEscaper.DEFAULT_ESCAPER.join(
                  IdConstants.TOKEN_SEPARATOR, IdConstants.BLIP_PREFIX, random64.next(6));
            }
          });
      OperationContextImpl context =
          new OperationContextImpl(
              converterManager.getEventDataConverter(protocolVersion), conversationUtil,
              new RobotWaveletData(waveletAndDeltas.getSnapshotAfterDeltas(),
                  waveletAndDeltas.getVersionAfterDeltas()));

      for (OperationRequest operation : operations) {
        // Get the operation of the author taking into account the "proxying for"
        // field.
        OperationUtil.executeOperation(operation, PASSIVE_API, context, robotId);
      }
      for (Entry<WaveletName, RobotWaveletData> waveletEntry :
          context.getOpenWavelets().entrySet()) {
        WaveletName waveletName = waveletEntry.getKey();
        log.info("waveletName=" + waveletName);
        RobotWaveletData w = waveletEntry.getValue();
        for (WaveletDelta delta : w.getDeltas()) {
          List<String> payloads = SERIALIZER.serializeDeltas(delta);
          ObjectSessionProto session = new ObjectSessionProtoGsonImpl();
          session.setObjectId(convSlobId.getId());
          session.setStoreType(convFacilities.getRootEntityKind());
          session.setClientId(clientIdGenerator.getIdForRobot(robotId).getId());
          ServerMutateRequest mutateRequest = new ServerMutateRequestGsonImpl();
          mutateRequest.setSession(session);
          mutateRequest.setVersion(waveletAndDeltas.getVersionAfterDeltas().getVersion());
          mutateRequest.addAllPayload(payloads);

          try {
            storeSelector.get(session.getStoreType()).getSlobStore()
                .mutateObject(mutateRequest);
          } catch (SlobNotFoundException e) {
            throw new RuntimeException(
                "Slob not found processing robot response: " + convSlobId, e);
          } catch (AccessDeniedException e) {
            // TODO: Make INFO once we confirm that it really only occurs if the
            // robot was removed
            log.log(Level.SEVERE, "Robot removed while processing response?! " + convSlobId, e);
          }
        }
      }
    }

    private void updateRobotHasBeenNotified(
        SlobId convSlobId, ParticipantId robotId, long lastSeen, WaveletAndDeltas waveletAndDeltas)
        throws IOException {
      log.info("Robot " + robotId + ": last seen " + lastSeen + ", now at "
          + waveletAndDeltas.getVersionAfterDeltas());
      versionDirectory.putWithoutTx(
          new RobotNotified(convSlobId, robotId, waveletAndDeltas.getVersionAfterDeltas()
              .getVersion()));
    }

    private EventMessageBundle generateEventMessageBundle(
        ParticipantId robotId, WaveletAndDeltas waveletAndDeltas) {
      ConversationUtil conversationUtil = new ConversationUtil(new IdHack.StubIdGenerator());
      EventGenerator generator =
          new EventGenerator(RobotName.fromAddress(robotId.getAddress()), conversationUtil);

      // TODO(ljv): Cache the capabilities?
      RobotCapabilities capabilities;
      try {
        capabilities = connector.fetchCapabilities(robotId, "");
      } catch (CapabilityFetchException e) {
        log.log(Level.WARNING, "Error occured fetching capabilities", e);
        return new EventMessageBundle(robotId.getAddress(), "");
      }

      EventDataConverter converter = converterManager.getEventDataConverter(
          capabilities.getProtocolVersion());
      return generator.generateEvents(
          waveletAndDeltas, capabilities.getCapabilitiesMap(), converter);
    }
  }

  @Override
  public void run() {
    try {
      GuiceSetup.getInjectorForRobotTask(robotId.getAddress())
          .createChildInjector(new RobotApiModule())
          .getInstance(Worker.class).run(convSlobId, newVersion, robotId);
    } catch (PermanentFailure e) {
      throw new RuntimeException(this + ": Worker failed", e);
    } catch (RetryableFailure e) {
      // TODO: retry
      throw new RuntimeException(this + ": Worker failed", e);
    } catch (IOException e) {
      // TODO: retry
      throw new RuntimeException(this + ": Worker failed", e);
    }
  }
}
