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

package com.google.walkaround.wave.server.gxp;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.walkaround.slob.shared.SlobId;

import javax.annotation.Nullable;

/**
 * Information {@link InboxFragment} needs about a wave.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class InboxDisplayRecord {

  private final SlobId slobId;
  private final String creator;
  private final String lastModified;
  private final String title;
  private final String snippetHtml;
  private final int blipCount;
  // Null means wave is read, non-null means unread.  0 means wave unread but
  // all blips read (participants unread).
  @Nullable private final Integer unreadCount;
  private final String link;

  public InboxDisplayRecord(SlobId slobId,
      String creator,
      String lastModified,
      String title,
      String snippetHtml,
      int blipCount,
      @Nullable Integer unreadCount,
      String link) {
    this.slobId = checkNotNull(slobId, "Null slobId");
    this.creator = checkNotNull(creator, "Null creator");
    this.lastModified = checkNotNull(lastModified, "Null lastModified");
    this.title = checkNotNull(title, "Null title");
    this.snippetHtml = checkNotNull(snippetHtml, "Null snippetHtml");
    this.blipCount = blipCount;
    this.unreadCount = unreadCount;
    this.link = checkNotNull(link, "Null link");
  }

  public SlobId getSlobId() {
    return slobId;
  }

  public String getCreator() {
    return creator;
  }

  public String getLastModified() {
    return lastModified;
  }

  public String getTitle() {
    return title;
  }

  public String getSnippetHtml() {
    return snippetHtml;
  }

  public int getBlipCount() {
    return blipCount;
  }

  @Nullable public Integer getUnreadCount() {
    return unreadCount;
  }

  public String getLink() {
    return link;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + slobId + ", "
        + creator + ", "
        + lastModified + ", "
        + title + ", "
        + snippetHtml + ", "
        + blipCount + ", "
        + unreadCount + ", "
        + link
        + ")";
  }

}
