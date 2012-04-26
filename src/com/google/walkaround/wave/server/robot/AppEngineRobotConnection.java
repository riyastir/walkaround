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

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;
import com.google.common.base.Charsets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.wave.api.robot.RobotConnection;
import com.google.wave.api.robot.RobotConnectionException;
import com.google.wave.api.robot.RobotConnectionUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class that can connect to robots using AppEngine's URL Fetch.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
public class AppEngineRobotConnection implements RobotConnection {

  private final URLFetchService service = URLFetchServiceFactory.getURLFetchService();

  @Override
  public String get(String url) throws RobotConnectionException {
    try {
      return asyncGet(url).get();
    } catch (InterruptedException e) {
      throw new RobotConnectionException("Unable to connect to robot", e);
    } catch (ExecutionException e) {
      throw new RobotConnectionException(
          "Exception occured when trying to connect to the robot", e);
    }
  }

  @Override
  public ListenableFuture<String> asyncGet(String url) throws RobotConnectionException {
    try {
      Future<HTTPResponse> responseFuture = service.fetchAsync(new URL(url));
      HTTPResponse response = responseFuture.get();
      // TODO(ljv): Figure out how to make Async.
      return Futures.immediateFuture(RobotConnectionUtil.validateAndReadResponse(
          url, response.getResponseCode(), response.getContent()));
    } catch (MalformedURLException e) {
      throw new RobotConnectionException("URL not valid", e);
    } catch (InterruptedException e) {
      throw new RobotConnectionException("Unable to fetch data from the robot", e);
    } catch (ExecutionException e) {
      throw new RobotConnectionException("Unable to fetch data from the robot", e);
    }
  }

  @Override
  public String postJson(String url, String jsonBody) throws RobotConnectionException {
    try {
      return asyncPostJson(url, jsonBody).get();
    } catch (InterruptedException e) {
      throw new RobotConnectionException("Unable to connect to robot", e);
    } catch (ExecutionException e) {
      throw new RobotConnectionException(
          "Exception occured when trying to connect to the robot", e);
    }
  }

  @Override
  public ListenableFuture<String> asyncPostJson(String url, String jsonBody)
      throws RobotConnectionException {
    try {

      HTTPRequest request = new HTTPRequest(new URL(url), HTTPMethod.POST);
      request.addHeader(new HTTPHeader("Content-Type", "text/json; charset=utf-8"));
      request.setPayload(jsonBody.getBytes(Charsets.UTF_8));

      Future<HTTPResponse> responseFuture = service.fetchAsync(request);

      // TODO(ljv): Figure out how to make Async.
      HTTPResponse response = responseFuture.get();
      return Futures.immediateFuture(RobotConnectionUtil.validateAndReadResponse(
          url, response.getResponseCode(), response.getContent()));

    } catch (MalformedURLException e) {
      throw new RobotConnectionException("URL not valid", e);
    } catch (InterruptedException e) {
      throw new RobotConnectionException("Unable to fetch data from the robot", e);
    } catch (ExecutionException e) {
      throw new RobotConnectionException("Unable to fetch data from the robot", e);
    }
  }
}
