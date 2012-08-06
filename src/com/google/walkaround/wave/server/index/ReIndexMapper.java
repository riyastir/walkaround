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

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.AppEngineMapper;
import com.google.inject.Inject;
import com.google.walkaround.slob.server.SlobFacilities;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.wave.server.GuiceSetup;
import com.google.walkaround.wave.server.conv.ConvStore;

import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mapreduce mapper that re-indexes all conversations.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class ReIndexMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(ReIndexMapper.class.getName());

  private static class Handler {
    @Inject @ConvStore SlobFacilities facilities;
    @Inject WaveIndexer indexer;

    void process(final Key key) throws PermanentFailure {
      new RetryHelper().run(new RetryHelper.VoidBody() {
          @Override public void run() throws PermanentFailure, RetryableFailure {
            SlobId objectId = facilities.parseRootEntityKey(key);
            // Update search index
            try {
              indexer.indexConversation(objectId);
            } catch (WaveletLockedException e) {
              log.log(Level.INFO, "Ignoring locked wavelet: " + objectId, e);
            }
          }
        });
    }
  }

  private static class ReIndexTask implements DeferredTask {
    private static final long serialVersionUID = 2322939563492683226L;
    private final Key convRootEntityKey;

    ReIndexTask(Key convRootEntityKey) {
      this.convRootEntityKey = checkNotNull(convRootEntityKey, "Null convRootEntityKey");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + convRootEntityKey + ")";
    }

    @Override public void run() {
      log.info(this + ": begin");
      try {
        GuiceSetup.getInjectorForTaskQueueTask().getInstance(Handler.class).process(convRootEntityKey);
      } catch (PermanentFailure e) {
        throw new RuntimeException(this + ": PermanentFailure", e);
      }
      log.info(this + ": success");
    }
  }

  @Override
  public void map(Key key, Entity value, Context context) throws IOException {
    context.getCounter(getClass().getSimpleName(), "wavelets").increment(1);
    log.info("Scheduling re-index task for " + key);
    QueueFactory.getQueue("reindex").add(TaskOptions.Builder.withPayload(new ReIndexTask(key)));
    context.getCounter(getClass().getSimpleName(), "re-index tasks scheduled").increment(1);
  }

}
