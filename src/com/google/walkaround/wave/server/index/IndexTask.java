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

import com.google.inject.Inject;
import com.google.walkaround.slob.server.PostCommitAction;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.SlobModel.ReadableSlob;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;

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
            indexer.indexConversation(slobId);
          }
        });
      } catch (PermanentFailure e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void unreliableImmediatePostCommit(SlobId slobId, long resultingVersion,
            ReadableSlob resultingState) {
      // XXX XXX remove
      reliableDelayedPostCommit(slobId);
      // nothing
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
            indexer.indexSupplement(slobId);
          }
        });
      } catch (PermanentFailure e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void unreliableImmediatePostCommit(SlobId slobId, long resultingVersion,
            ReadableSlob resultingState) {
      // XXX XXX remove
      reliableDelayedPostCommit(slobId);
      // nothing
    }
  }

}
