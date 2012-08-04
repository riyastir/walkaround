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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Class that can connect to robots using AppEngine's URL Fetch.
 *
 * @author ljv@google.com (Lennard de Rijk)
 */
class AppEngineRobotConnection implements RobotConnection {

  private static final URLFetchService SERVICE = URLFetchServiceFactory.getURLFetchService();

  private URL makeUrl(String urlString) throws RobotConnectionException {
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      throw new RobotConnectionException("Malformed URL: " + urlString, e);
    }
  }

  private HTTPResponse fetch(HTTPRequest req) throws RobotConnectionException {
    try {
      return SERVICE.fetch(req);
    } catch (IOException e) {
      throw new RobotConnectionException("IOException communicating with robot; req=" + req, e);
    }
  }

  @Override
  public String get(String url) throws RobotConnectionException {
    HTTPRequest request = new HTTPRequest(makeUrl(url), HTTPMethod.GET);
    HTTPResponse response = fetch(request);
    return RobotConnectionUtil.validateAndReadResponse(
        url, response.getResponseCode(), response.getContent());
  }

  @Override
  public ListenableFuture<String> asyncGet(String url) throws RobotConnectionException {
    // This is synchronous rather than asynchronous, but that should be OK
    // because we use a separate task queue task for each notification, so
    // blocking here doesn't block any other work.
    return Futures.immediateFuture(get(url));
  }

  @Override
  public String postJson(String url, String jsonBody) throws RobotConnectionException {
    HTTPRequest request = new HTTPRequest(makeUrl(url), HTTPMethod.POST);
    request.addHeader(new HTTPHeader("Content-Type", JSON_CONTENT_TYPE));
    request.setPayload(jsonBody.getBytes(Charsets.UTF_8));
    HTTPResponse response = fetch(request);
    return RobotConnectionUtil.validateAndReadResponse(
        url, response.getResponseCode(), response.getContent());
  }

  @Override
  public ListenableFuture<String> asyncPostJson(String url, String jsonBody)
      throws RobotConnectionException {
    // This is synchronous rather than asynchronous, but that should be OK
    // because we use a separate task queue task for each notification, so
    // blocking here doesn't block any other work.
    return Futures.immediateFuture(postJson(url, jsonBody));
  }
}
