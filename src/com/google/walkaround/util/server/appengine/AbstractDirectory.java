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

package com.google.walkaround.util.server.appengine;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.common.base.Preconditions;
import com.google.walkaround.util.server.RetryHelper;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A directory in the datastore that stores a set of T, where each T contains an
 * id of type I.  Each entity is its own entity group.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> entry type
 * @param <I> id type
 */
public abstract class AbstractDirectory<T, I> {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(AbstractDirectory.class.getName());

  private final CheckedDatastore datastore;
  private final String entityKind;

  public AbstractDirectory(CheckedDatastore datastore, String entityKind) {
    Preconditions.checkNotNull(datastore, "Null datastore");
    Preconditions.checkNotNull(entityKind, "Null entityKind");
    this.datastore = datastore;
    this.entityKind = entityKind;
  }

  protected abstract String serializeId(I id);
  protected abstract I getId(T e);
  protected abstract void populateEntity(T in, Entity out);
  protected abstract T parse(Entity e);

  public Key makeKey(I id) {
    return KeyFactory.createKey(entityKind, serializeId(id));
  }

  @Nullable public T get(CheckedTransaction tx, I id) throws PermanentFailure, RetryableFailure {
    Key key = makeKey(id);
    Entity result = tx.get(key);
    if (result != null) {
      log.info("Looked up " + key + ", found " + result);
      return parse(result);
    } else {
      log.info("Looked up " + key + ", not found");
      return null;
    }
  }

  @Nullable public T getWithoutTx(I id) throws IOException {
    try {
      CheckedTransaction tx = datastore.beginTransaction();
      try {
        return get(tx, id);
      } finally {
        tx.rollback();
      }
    } catch (PermanentFailure e) {
      log.log(Level.SEVERE, "Failed to look up " + id, e);
      throw new IOException(e);
    } catch (RetryableFailure e) {
      log.log(Level.SEVERE, "Failed to look up " + id, e);
      throw new IOException(e);
    }
  }

  public void put(CheckedTransaction tx, T newEntry) throws PermanentFailure, RetryableFailure {
    Preconditions.checkNotNull(tx, "Null tx");
    Preconditions.checkNotNull(newEntry, "Null newEntry");
    Key key = makeKey(getId(newEntry));
    Entity newEntity = new Entity(key);
    populateEntity(newEntry, newEntity);
    // For now, we verify that it parses with no exceptions.  We can turn
    // this off if it's too expensive.
    parse(newEntity);
    log.info("Putting " + newEntity + " in " + tx);
    tx.put(newEntity);
  }

  public void putWithoutTx(final T newEntry) throws IOException {
    Preconditions.checkNotNull(newEntry, "Null newEntry");
    try {
      new RetryHelper().run(new RetryHelper.VoidBody() {
        @Override public void run() throws RetryableFailure, PermanentFailure {
          CheckedTransaction tx = datastore.beginTransaction();
          try {
            put(tx, newEntry);
            tx.commit();
            log.info("Committed " + tx);
          } finally {
            tx.close();
          }
        }
      });
    } catch (PermanentFailure e) {
      throw new IOException(e);
    }
  }

  /**
   * Transactionally checks if an entry with the same key as newEntry exists,
   * and adds newEntry if not.  If an entry already exists, returns the existing
   * entry; otherwise (i.e., if newEntry was added), returns null.
   */
  // NOTE(danilatos): Don't be tempted to implement a version of this method that
  // accepts a Provider or something in order to only create the newEntry if one
  // does not exist in the datastore, because megastore transactions run in parallel
  // and just fail on commit (there is no locking).
  // Instead, use get() and then getOrAdd() if get returns null.

  // Is this method worthwhile, given that a caller can just use get() and put()
  // and manage their own transaction?
  @Nullable public T getOrAdd(final T newEntry) throws IOException {
    Preconditions.checkNotNull(newEntry, "Null newEntry");
    try {
      return new RetryHelper().run(new RetryHelper.Body<T>() {
        @Override public T run() throws RetryableFailure, PermanentFailure {
          CheckedTransaction tx = datastore.beginTransaction();
          try {
            Entity existing = tx.get(makeKey(getId(newEntry)));
            if (existing != null) {
              log.info("Read " + existing + " in " + tx);
              return parse(existing);
            } else {
              put(tx, newEntry);
              tx.commit();
              log.info("Committed " + tx);
              return null;
            }
          } finally {
            tx.close();
          }
        }
      });
    } catch (PermanentFailure e) {
      throw new IOException(e);
    }
  }

}
