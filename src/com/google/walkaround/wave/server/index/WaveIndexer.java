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

import com.google.appengine.api.search.AddDocumentsException;
import com.google.appengine.api.search.AddDocumentsResponse;
import com.google.appengine.api.search.Consistency;
import com.google.appengine.api.search.Document;
import com.google.appengine.api.search.Field;
import com.google.appengine.api.search.Index;
import com.google.appengine.api.search.IndexManager;
import com.google.appengine.api.search.IndexSpec;
import com.google.appengine.api.search.OperationResult;
import com.google.appengine.api.search.SearchException;
import com.google.appengine.api.search.SearchQueryException;
import com.google.appengine.api.search.SearchRequest;
import com.google.appengine.api.search.SearchResponse;
import com.google.appengine.api.search.SearchResult;
import com.google.appengine.api.search.StatusCode;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.repackaged.com.google.common.collect.Iterables;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.walkaround.slob.server.MutationLog;
import com.google.walkaround.slob.server.MutationLog.MutationLogFactory;
import com.google.walkaround.slob.shared.MessageException;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.StateAndVersion;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.shared.RandomBase64Generator;
import com.google.walkaround.wave.server.auth.AccountStore;
import com.google.walkaround.wave.server.conv.ConvStore;
import com.google.walkaround.wave.server.model.ServerMessageSerializer;
import com.google.walkaround.wave.server.model.TextRenderer;
import com.google.walkaround.wave.server.udw.UdwStore;
import com.google.walkaround.wave.server.udw.UserDataWaveletDirectory;
import com.google.walkaround.wave.server.udw.UserDataWaveletDirectory.ConvUdwMapping;
import com.google.walkaround.wave.shared.IdHack;
import com.google.walkaround.wave.shared.WaveSerializer;

import org.waveprotocol.wave.model.conversation.BlipIterators;
import org.waveprotocol.wave.model.conversation.Conversation;
import org.waveprotocol.wave.model.conversation.ConversationBlip;
import org.waveprotocol.wave.model.conversation.WaveBasedConversationView;
import org.waveprotocol.wave.model.document.operation.DocInitialization;
import org.waveprotocol.wave.model.document.operation.automaton.DocumentSchema;
import org.waveprotocol.wave.model.id.IdGenerator;
import org.waveprotocol.wave.model.id.WaveId;
import org.waveprotocol.wave.model.id.WaveletId;
import org.waveprotocol.wave.model.id.WaveletName;
import org.waveprotocol.wave.model.supplement.PrimitiveSupplement;
import org.waveprotocol.wave.model.supplement.Supplement;
import org.waveprotocol.wave.model.supplement.SupplementImpl;
import org.waveprotocol.wave.model.supplement.WaveletBasedSupplement;
import org.waveprotocol.wave.model.wave.ParticipantId;
import org.waveprotocol.wave.model.wave.data.DocumentFactory;
import org.waveprotocol.wave.model.wave.data.impl.ObservablePluggableMutableDocument;
import org.waveprotocol.wave.model.wave.data.impl.WaveletDataImpl;
import org.waveprotocol.wave.model.wave.opbased.OpBasedWavelet;
import org.waveprotocol.wave.model.wave.opbased.WaveViewImpl;
import org.waveprotocol.wave.model.wave.opbased.WaveViewImpl.WaveletConfigurator;
import org.waveprotocol.wave.model.wave.opbased.WaveViewImpl.WaveletFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author danilatos@google.com (Daniel Danilatos)
 *
 */
public class WaveIndexer {
  public static class UserIndexEntry {
    private final SlobId objectId;
    private final ParticipantId creator;
    private final String title;
    private final String snippet;
    private final long lastModifiedMillis;
    private final int blipCount;
    private final @Nullable Integer unreadCount;

    public UserIndexEntry(SlobId objectId,
        ParticipantId creator,
        String title,
        String snippet,
        long lastModifiedMillis,
        int blipCount,
        @Nullable Integer unreadCount) {
      this.objectId = checkNotNull(objectId, "Null objectId");
      this.creator = checkNotNull(creator, "Null creator");
      this.title = checkNotNull(title, "Null title");
      this.snippet = checkNotNull(snippet, "Null snippet");
      this.lastModifiedMillis = lastModifiedMillis;
      this.blipCount = blipCount;
      this.unreadCount = unreadCount;
    }

    public SlobId getObjectId() {
      return objectId;
    }

    public ParticipantId getCreator() {
      return creator;
    }

    public String getTitle() {
      return title;
    }

    public String getSnippetHtml() {
      return snippet;
    }

    public long getLastModifiedMillis() {
      return lastModifiedMillis;
    }

    public int getBlipCount() {
      return blipCount;
    }

    /**
     * Null if it's completely read.
     */
    @Nullable public Integer getUnreadCount() {
      return unreadCount;
    }

    @Override public String toString() {
      return "IndexEntry("
          + objectId + ", "
          + creator + ", "
          + title + ", "
          + snippet + ", "
          + lastModifiedMillis
          + ")";
    }

    @Override public final boolean equals(Object o) {
      if (o == this) { return true; }
      if (!(o instanceof UserIndexEntry)) { return false; }
      UserIndexEntry other = (UserIndexEntry) o;
      return lastModifiedMillis == other.lastModifiedMillis
          && Objects.equal(objectId, other.objectId)
          && Objects.equal(creator, other.creator)
          && Objects.equal(title, other.title)
          && Objects.equal(snippet, other.snippet);
    }

    @Override public final int hashCode() {
      return Objects.hashCode(objectId, creator, title, snippet, lastModifiedMillis);
    }
  }

  private static class ConvFields {
    SlobId slobId;
    String content;
    String title;
    String creator;
    String lastModified;
    int version;
    WaveletId waveletId;
    Conversation model;
    int blipCount;
  }

  private static final Logger log = Logger.getLogger(WaveIndexer.class.getName());

  private static final String USER_WAVE_INDEX_PREFIX = "USRIDX2-";

  private final WaveSerializer serializer;
  private final IndexManager indexManager;
  private final CheckedDatastore datastore;
  private final MutationLogFactory convStore;
  private final MutationLogFactory udwStore;
  private final UserDataWaveletDirectory udwDirectory;
  private final AccountStore users;
  private final RandomBase64Generator random;

  @Inject
  public WaveIndexer(CheckedDatastore datastore,
      @ConvStore MutationLogFactory convStore,
      @UdwStore MutationLogFactory udwStore,
      UserDataWaveletDirectory udwDirectory,
      IndexManager indexManager,
      AccountStore users,
      RandomBase64Generator random) {
    this.datastore = datastore;
    this.convStore = convStore;
    this.udwStore = udwStore;
    this.udwDirectory = udwDirectory;
    this.indexManager = indexManager;
    this.users = users;
    this.random = random;
    this.serializer = new WaveSerializer(
        new ServerMessageSerializer(), new DocumentFactory<ObservablePluggableMutableDocument>() {

          @Override
          public ObservablePluggableMutableDocument create(
              WaveletId waveletId, String docId, DocInitialization content) {
            return new ObservablePluggableMutableDocument(
                DocumentSchema.NO_SCHEMA_CONSTRAINTS, content);
          }
        });
  }

  /**
   * Indexes the wave for all users
   */
  public void indexConversation(SlobId convId) throws RetryableFailure, PermanentFailure {
    // TODO(danilatos): Handle waves for participants that have been removed.
    // Currently they will remain in the inbox but they won't have access until
    // we implement snapshotting of the waves at the point the participants
    // were removed (or similar).
    log.info("Indexing conversation for all participants");
    ConvFields convFields = getConvFields(convId);

    List<ConvUdwMapping> convUserMappings = udwDirectory.getAllUdwIds(convId);
    Map<ParticipantId, SlobId> participantsToUdws = Maps.newHashMap();
    for (ConvUdwMapping m : convUserMappings) {
      participantsToUdws.put(users.get(m.getKey().getUserId()).getParticipantId(), m.getUdwId());
    }

    for (ParticipantId participant : convFields.model.getParticipantIds()) {
      SlobId udwId = participantsToUdws.get(participant);
      log.info("Indexing " + convId.getId() + " for " + participant.getAddress() +
          (udwId != null ? " with udw " + udwId : ""));

      Supplement supplement = udwId == null ? null : getSupplement(load(udwStore, udwId));
      index(convFields, participant, supplement);
    }
  }

  /**
   * Indexes the wave only for the user of the given user data wavelet
   */
  public void indexSupplement(SlobId udwId) throws PermanentFailure, RetryableFailure {
    log.info("Indexing conversation for participant with udwId " + udwId);
    PrimitiveSupplement supplement = getPrimitiveSupplement(load(udwStore, udwId));
    // NOTE(danilatos): This trick (grabbing the only "read wavelet" in the supplement)
    // won't work if we ever support private replies...
    // it only works if there is only 1 conversation per wave.
    for (WaveletId waveletId : supplement.getReadWavelets()) {
      SlobId convId = IdHack.objectIdFromWaveletId(waveletId);

      // Is there a better way to get the participant id from the supplement?
      List<ConvUdwMapping> convUserMappings = udwDirectory.getAllUdwIds(convId);
      for (ConvUdwMapping m : convUserMappings) {
        if (m.getUdwId().equals(udwId)) {
          index(getConvFields(convId), users.get(m.getKey().getUserId()).getParticipantId(),
              new SupplementImpl(supplement));

          return;
        }
      }
      throw new RuntimeException("Unexpectedly did not find the supplement " + udwId +
          " associated with its conversation");
    }
  }

  private ConvFields getConvFields(SlobId convId) throws PermanentFailure, RetryableFailure {
    ConvFields fields = new ConvFields();
    WaveletDataImpl convData = load(convStore, convId);

    fields.slobId = convId;
    fields.content = TextRenderer.renderToText(convData);
    fields.title = Util.extractTitle(convData);
    fields.creator = convData.getCreator().getAddress();
    fields.lastModified = convData.getLastModifiedTime() + "";
    fields.version = (int) convData.getVersion();
    fields.waveletId = convData.getWaveletId();
    fields.model = getConversation(convData);
    fields.blipCount = countBlips(fields.model);

    return fields;
  }

  private void index(ConvFields conv, ParticipantId user, @Nullable Supplement supplement)
      throws RetryableFailure, PermanentFailure {

    boolean isArchived = false;
    boolean isFollowed = true;
    @Nullable Integer unreadCount = conv.blipCount;
    if (supplement != null) {
      unreadCount = unreadCount(conv.model, conv.waveletId, conv.version, supplement);
      isArchived = supplement.isArchived(conv.waveletId, conv.version);
      isFollowed = supplement.isFollowed(true);
    }

    log.info("Unread count: " + unreadCount + " has supplement: " + (supplement != null));

    Document.Builder builder = Document.newBuilder();
    builder.setId(conv.slobId.getId());
    builder.addField(Field.newBuilder().setName("content").setText(conv.content));
    builder.addField(Field.newBuilder().setName("title").setText(conv.title));
    builder.addField(Field.newBuilder().setName("creator").setText(conv.creator));
    builder.addField(Field.newBuilder().setName("modified").setText(conv.lastModified));
    builder.addField(Field.newBuilder().setName("in").setText("inbox"));
    builder.addField(Field.newBuilder().setName("blips").setText("" + conv.blipCount)); //use num?
    builder.addField(Field.newBuilder().setName("unread").setText(
        unreadCount == null ? "no" : ("" + unreadCount)));
    builder.addField(Field.newBuilder().setName("is").setText(
        (unreadCount == null ? "read " : "unread ")
        + (isArchived ? "archived " : "")
        + (isFollowed ? "followed " : ""))
        );
    Document doc = builder.build();

    Index idx = getIndex(user);

    log.info("Saving index document  " + describe(doc));

    // TODO(danilatos): Factor out all the error handling?
    AddDocumentsResponse resp;
    try {
      resp = idx.add(doc);
    } catch (AddDocumentsException e) {
      throw new RetryableFailure("Error indexing " + conv.slobId, e);
    }
    for (OperationResult result : resp.getResults()) {
      if (!result.getCode().equals(StatusCode.OK)) {
        throw new RetryableFailure("Error indexing " + conv.slobId + ", " + result.getMessage());
      }
    }
  }

  private int countBlips(Conversation conv) {
    return Iterables.size(BlipIterators.breadthFirst(conv));
  }

  /**
   * Returns the number of unread blips, and returns 0 if there are no unread
   * blips but the participant list or any other non-blip part of the wave is
   * "unread".
   *
   * Returns null if the conversation is fully read.
   */
  private Integer unreadCount(Conversation conv, WaveletId convId, int convVersion,
      Supplement supplement) {
    int unreadBlips = 0;
    for (ConversationBlip blip : BlipIterators.breadthFirst(conv)) {
      log.info("Seen wavelets: " + supplement.getSeenWavelets());
      log.info("Conv wavelet: " + convId);
      if (supplement.isBlipUnread(convId, blip.getId(), (int) blip.getLastModifiedVersion())) {
        unreadBlips++;
      }
    }

    if (unreadBlips > 0) {
      return unreadBlips;
    }
//    TODO(danilatos): Get this bit to work...  probably something wrong with the version.
//    if (supplement.isParticipantsUnread(convId, convVersion)) {
//      return 0;
//    }

    return null;
  }

  private static final WaveletFactory<OpBasedWavelet> STUB_FACTORY =
    new WaveletFactory<OpBasedWavelet>() {
        @Override public OpBasedWavelet create(WaveId waveId, WaveletId id, ParticipantId creator) {
          throw new UnsupportedOperationException();
        }
      };
  private Conversation getConversation(WaveletDataImpl convData) {
    IdGenerator idGenerator = new IdHack.MinimalIdGenerator(
            convData.getWaveletId(),
            IdHack.FAKE_WAVELET_NAME.waveletId,
            random);
    WaveViewImpl<OpBasedWavelet> waveView = WaveViewImpl.create(STUB_FACTORY,
        convData.getWaveId(), idGenerator,
        ParticipantId.ofUnsafe("fake@fake.com"),
        WaveletConfigurator.ERROR);
    waveView.addWavelet(OpBasedWavelet.createReadOnly(convData));
    WaveBasedConversationView convView = WaveBasedConversationView.create(
        waveView, idGenerator);

    return convView.getRoot();
  }

  private Supplement getSupplement(WaveletDataImpl udwData) {
    return new SupplementImpl(getPrimitiveSupplement(udwData));
  }

  private PrimitiveSupplement getPrimitiveSupplement(WaveletDataImpl udwData) {
    return WaveletBasedSupplement.create(OpBasedWavelet.createReadOnly(udwData));
  }

  private WaveletDataImpl load(MutationLogFactory store, SlobId id)
      throws PermanentFailure, RetryableFailure {

    StateAndVersion raw;
    CheckedTransaction tx = datastore.beginTransaction();
    try {
      MutationLog l = store.create(tx, id);
      raw = l.reconstruct(null);
    } finally {
      tx.rollback();
    }

    WaveletName waveletName = IdHack.convWaveletNameFromConvObjectId(id);

    WaveletDataImpl waveletData = deserializeWavelet(waveletName,
        raw.getState().snapshot());

    if (raw.getVersion() != waveletData.getVersion()) {
      throw new AssertionError("Raw version " + raw.getVersion() +
          " does not match wavelet version " + waveletData.getVersion());
    }


    return waveletData;
  }

  private Index getIndex(ParticipantId participant) {
    return indexManager.getIndex(
        IndexSpec.newBuilder().setName(getIndexName(participant))
        // We could consider making this global, though the documentation
        // says not more than 1 update per second, which is a bit borderline.
        .setConsistency(Consistency.PER_DOCUMENT).build());
  }

  private String getIndexName(ParticipantId participant) {
    // NOTE(danilatos): This is not robust against email addresses being recycled.
    return USER_WAVE_INDEX_PREFIX + participant.getAddress();
  }

  private WaveletDataImpl deserializeWavelet(WaveletName waveletName, String snapshot) {
    try {
      return serializer.deserializeWavelet(waveletName, snapshot);
    } catch (MessageException e) {
      throw new RuntimeException("Invalid snapshot for " + waveletName, e);
    }
  }

  public List<UserIndexEntry> findWaves(ParticipantId user, String query, int offset, int limit)
      throws IOException {

    // Trailing whitespace causes parse exceptions...
    query = query.trim();

    log.info("Searching for " + query);
    SearchRequest request;
    try {
      request = SearchRequest.newBuilder()
          .setQuery(query)
          .setFieldsToSnippet("content")
          .setOffset(offset)
          .setLimit(limit)
          .build();
    } catch (SearchQueryException e) {
      log.log(Level.WARNING, "Problem building query " + query, e);
      return Collections.emptyList();
    }

    // TODO(danilatos): Figure out why this throws unchecked exceptions for bad searches
    // but also returns status codes...?
    SearchResponse response;
    try {
      response = getIndex(user).search(request);
    } catch (SearchException e) {
      // This seems to happen for certain queries that the user can type, for some
      // reason they are not caught at the parse stage above with SearchQueryException,
      // so let's just log and return.
      log.log(Level.WARNING, "Problem executing query " + query, e);
      return Collections.emptyList();
    }
    StatusCode responseCode = response.getOperationResult().getCode();
    if (!StatusCode.OK.equals(responseCode)) {
      throw new IOException("Search fail for query '" + query + "'"
          + ", response: " + responseCode + " " + response.getOperationResult().getMessage());
    }
    List<UserIndexEntry> entries = Lists.newArrayList();
    for (SearchResult result : response) {
      Document doc = result.getDocument();

      List<Field> expressions = result.getExpressions();
      assert expressions.size() == 1;
      String snippetHtml = expressions.get(0).getHTML();
      if (snippetHtml == null) {
        log.warning("No snippet in " + expressions + " in " + result);
        snippetHtml = "";
      }

      // Workaround for bug where all fields come back empty in local dev mode.
      boolean HACK = SystemProperty.environment.value()
          == SystemProperty.Environment.Value.Development;
      String unreadCount;
      if (HACK) {
        int c = new Random().nextInt(5);
        unreadCount = c == 0 ? "no" : "" + c * 7;
      } else {
        unreadCount = getField(doc, "unread");
      }
      entries.add(new UserIndexEntry(
          new SlobId(doc.getId()),
          ParticipantId.ofUnsafe(HACK ? "hack@hack.com" : getField(doc, "creator")),
          HACK ? "XXX" : getField(doc, "title"),
          snippetHtml,
          Long.parseLong(HACK ? "1" : getField(doc, "modified")),
          Integer.parseInt(HACK ? "23" : getField(doc, "blips")),
          "no".equals(unreadCount) ? null : Integer.parseInt(unreadCount)
          ));
    }
    return entries;
  }

  private String getField(Document doc, String field) {
    Iterable<Field> fields = doc.getField(field);
    if (fields == null) {
      throw new RuntimeException("No field " + field + " in doc " + describe(doc));
    }
    for (Field f : fields) {
      return f.getText();
    }
    throw new RuntimeException("Empty field list for " + field + " in doc " + describe(doc));
  }

  private String describe(Document doc) {
    return "doc[" + doc.getId() + " with " + doc.getFields() + "]";
  }
}