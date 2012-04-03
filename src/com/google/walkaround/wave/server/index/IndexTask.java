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

package com.google.walkaround.wave.server.index;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.walkaround.slob.server.PostCommitAction;
import com.google.walkaround.slob.server.PostCommitActionQueue;
import com.google.walkaround.slob.server.PreCommitAction;
import com.google.walkaround.slob.shared.ChangeData;
import com.google.walkaround.slob.shared.MessageException;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.SlobModel.ReadableSlob;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.wave.server.GuiceSetup;
import com.google.walkaround.wave.server.model.ServerMessageSerializer;
import com.google.walkaround.wave.shared.WaveSerializer;

import org.waveprotocol.wave.model.operation.wave.RemoveParticipant;
import org.waveprotocol.wave.model.operation.wave.WaveletOperation;
import org.waveprotocol.wave.model.wave.ParticipantId;

import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author danilatos@google.com
 */
public class IndexTask {
  private static final Logger log = Logger.getLogger(IndexTask.class.getName());

  public static class Conv implements PostCommitAction {
    @Inject private WaveIndexer indexer;

    @Override
    public void reliableDelayedPostCommit(final SlobId slobId) {
      try {
        new RetryHelper().run(new RetryHelper.VoidBody() {
          @Override
          public void run() throws RetryableFailure, PermanentFailure {
            try {
              indexer.indexConversation(slobId);
            } catch (WaveletLockedException e) {
              log.log(Level.SEVERE, "Post-commit on locked conv wavelet " + slobId, e);
            }
          }
        });
      } catch (PermanentFailure e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void unreliableImmediatePostCommit(SlobId slobId, long resultingVersion,
        ReadableSlob resultingState) {
      // nothing
    }
  }

  private static final WaveSerializer SERIALIZER = new WaveSerializer(new ServerMessageSerializer());

  private static class HandleRemovedParticipantsTask implements DeferredTask {
    private static final long serialVersionUID = 550383067366922381L;

    private final SlobId convId;
    private final Set<ParticipantId> removedParticipants;

    public HandleRemovedParticipantsTask(SlobId convId,
        Set<ParticipantId> removedParticipants) {
      this.convId = checkNotNull(convId, "Null convId");
      this.removedParticipants = checkNotNull(removedParticipants, "Null removedParticipants");
    }

    @Override public void run() {
      try {
        new RetryHelper().run(new RetryHelper.VoidBody() {
          @Override
          public void run() throws RetryableFailure, PermanentFailure {
            try {
              GuiceSetup.getInjectorForTaskQueueTask().getInstance(WaveIndexer.class)
                  .unindexConversationForPossiblyRemovedParticipants(
                      convId, removedParticipants);
            } catch (WaveletLockedException e) {
              throw new RuntimeException(
                  "Failed to unindex " + convId + " for " + removedParticipants, e);
            }
          }
        });
      } catch (PermanentFailure e) {
        throw new RuntimeException(
            "Failed to unindex " + convId + " for " + removedParticipants, e);
      }
    }
  }

  public static class ConvPreCommit implements PreCommitAction {
    @Inject @PostCommitActionQueue Queue postCommitActionQueue;

    @Override
    public void run(CheckedTransaction tx, SlobId convId, List<ChangeData<String>> newDeltas,
        long resultingVersion, ReadableSlob resultingState)
        throws RetryableFailure, PermanentFailure {
      ImmutableSet.Builder<ParticipantId> out = ImmutableSet.builder();
      for (ChangeData<String> delta : newDeltas) {
        WaveletOperation op;
        try {
          op = SERIALIZER.deserializeDelta(delta.getPayload());
        } catch (MessageException e) {
          throw new RuntimeException("Malformed op: " + delta, e);
        }
        if (op instanceof RemoveParticipant) {
          out.add(((RemoveParticipant) op).getParticipantId());
        }
      }
      ImmutableSet<ParticipantId> removedParticipants = out.build();
      if (!removedParticipants.isEmpty()) {
        log.info("Will unindex " + convId + " for " + removedParticipants);
        tx.enqueueTask(postCommitActionQueue,
            TaskOptions.Builder.withPayload(
                new HandleRemovedParticipantsTask(convId, removedParticipants)));
      }
    }
  }

  public static class Udw implements PostCommitAction {
    @Inject private WaveIndexer indexer;

    @Override
    public void reliableDelayedPostCommit(final SlobId slobId) {
      try {
        new RetryHelper().run(new RetryHelper.VoidBody() {
          @Override
          public void run() throws RetryableFailure, PermanentFailure {
            try {
              indexer.indexSupplement(slobId);
            } catch (WaveletLockedException e) {
              log.log(Level.SEVERE, "Post-commit on udw when conv wavelet is locked: " + slobId, e);
            }
          }
        });
      } catch (PermanentFailure e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void unreliableImmediatePostCommit(SlobId slobId, long resultingVersion,
         ReadableSlob resultingState) {
      // nothing
    }
  }

}
