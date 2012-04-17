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

package com.google.walkaround.wave.server.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.waveprotocol.wave.model.conversation.BlipIterators;
import org.waveprotocol.wave.model.conversation.Conversation;
import org.waveprotocol.wave.model.conversation.ConversationBlip;
import org.waveprotocol.wave.model.document.operation.AnnotationBoundaryMap;
import org.waveprotocol.wave.model.document.operation.Attributes;
import org.waveprotocol.wave.model.document.operation.DocInitialization;
import org.waveprotocol.wave.model.document.operation.DocInitializationCursor;

/**
 * Renders a wave as HTML text, for indexing. The rendering is just the text of
 * each document, arbitrarily ordered.
 *
 * @author hearnden@google.com (David Hearnden)
 */
public final class TextRenderer {

  private TextRenderer() {}

  /**
   * Renders a wavelet into text.
   */
  public static String renderToText(Conversation conv) {
    checkNotNull(conv, "Null conv");
    StringBuilder b = new StringBuilder();
    for (ConversationBlip blip : BlipIterators.breadthFirst(conv)) {
      render(blip.getContent().toInitialization(), b);
    }
    return b.toString();
  }

  /**
   * Renders a document as a paragraph of plain text.
   */
  public static void render(DocInitialization doc, final StringBuilder out) {
    checkNotNull(doc, "Null doc");
    checkNotNull(out, "Null out");
    doc.apply(new DocInitializationCursor() {
      @Override
      public void characters(String chars) {
        out.append(chars);
      }

      @Override
      public void elementStart(String type, Attributes attrs) {
        out.append(' ');
      }

      @Override
      public void elementEnd() {
        out.append(' ');
      }

      @Override
      public void annotationBoundary(AnnotationBoundaryMap map) {
        // Ignore
      }
    });
    out.append("\n");
  }
}
