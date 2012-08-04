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

package com.google.walkaround.wave.server.googleimport;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.AppEngineMapper;
import com.google.inject.Inject;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.wave.server.GuiceSetup;

import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Mapreduce mapper that schedules new a task queue task for every pending
 * import task.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class RetryTasksMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(RetryTasksMapper.class.getName());

  private static class Handler {
    @Inject PerUserTable perUserTable;

    void process(Context context, final Entity entity) throws PermanentFailure {
      new RetryHelper().run(new RetryHelper.VoidBody() {
          @Override public void run() throws PermanentFailure, RetryableFailure {
            perUserTable.rescheduleExistingTask(perUserTable.parseTaskEntity(entity));
          }
        });
    }
  }

  @Override
  public void map(Key key, Entity entity, Context context) throws IOException {
    context.getCounter(getClass().getSimpleName(), "entities-seen").increment(1);
    log.info("Re-indexing " + key);
    try {
      GuiceSetup.getInjectorForTaskQueueTask().getInstance(Handler.class).process(context, entity);
    } catch (PermanentFailure e) {
      throw new IOException("PermanentFailure re-indexing key " + key, e);
    }
    context.getCounter(getClass().getSimpleName(), "entities-processed").increment(1);
  }

}
