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

package com.google.walkaround.wave.server;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceConfig;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.ImplicitTransactionManagementPolicy;
import com.google.appengine.api.datastore.ReadPolicy;
import com.google.appengine.api.utils.SystemProperty;
import com.google.apphosting.api.ApiProxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Provides datastore instance to use.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class DatastoreProvider {

  private DatastoreProvider() {}

  static {
    // Weird contortions to get XG transactions to work when running locally.
    // TODO(ohler): Figure out if there's a better way.
    if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Development) {
      // We use reflection to avoid having to depend on testing jars.
      Object apiProxy = ApiProxy.getDelegate();
      @SuppressWarnings("rawtypes")
      Class[] paramTypes = new Class[] { String.class, String.class };
      Method setProperty;
      try {
        setProperty = apiProxy.getClass().getMethod("setProperty", paramTypes);
      } catch (SecurityException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
      setProperty.setAccessible(true);
      try {
        setProperty.invoke(apiProxy,
            // This string literal is from
            // DefaultHighRepJobPolicy.UNAPPLIED_JOB_PERCENTAGE_PROPERTY
            // but we inline it here to avoid depending on the local development
            // JARs that contain that class.
            "datastore.default_high_rep_job_policy_unapplied_job_pct",
            "30");
      } catch (InvocationTargetException e) {
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final DatastoreService STRONG_READS =
      DatastoreServiceFactory.getDatastoreService(DatastoreServiceConfig.Builder
          .withDeadline(5 /*seconds*/)
          .implicitTransactionManagementPolicy(ImplicitTransactionManagementPolicy.NONE)
          .readPolicy(new ReadPolicy(ReadPolicy.Consistency.STRONG)));

  private static final DatastoreService EVENTUAL_READS =
      DatastoreServiceFactory.getDatastoreService(DatastoreServiceConfig.Builder
          .withDeadline(5 /*seconds*/)
          .implicitTransactionManagementPolicy(ImplicitTransactionManagementPolicy.NONE)
          .readPolicy(new ReadPolicy(ReadPolicy.Consistency.EVENTUAL)));

  public static DatastoreService strongReads() {
    return STRONG_READS;
  }

  public static DatastoreService eventualReads() {
    return EVENTUAL_READS;
  }

}
