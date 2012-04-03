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
import com.google.appengine.api.search.SortSpec;
import com.google.appengine.api.search.SortSpec.SortDirection;
import com.google.appengine.api.search.StatusCode;
import com.google.appengine.api.search.checkers.FieldChecker;
import com.google.appengine.api.utils.SystemProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.walkaround.proto.ConvMetadata;
import com.google.walkaround.proto.gson.ObsoleteWaveletMetadataGsonImpl;
import com.google.walkaround.slob.server.GsonProto;
import com.google.walkaround.slob.server.MutationLog;
import com.google.walkaround.slob.server.MutationLog.MutationLogFactory;
import com.google.walkaround.slob.shared.MessageException;
import com.google.walkaround.slob.shared.SlobId;
import com.google.walkaround.slob.shared.StateAndVersion;
import com.google.walkaround.util.server.RetryHelper.PermanentFailure;
import com.google.walkaround.util.server.RetryHelper.RetryableFailure;
import com.google.walkaround.util.server.appengine.CheckedDatastore;
import com.google.walkaround.util.server.appengine.CheckedDatastore.CheckedTransaction;
import com.google.walkaround.util.shared.Assert;
import com.google.walkaround.util.shared.RandomBase64Generator;
import com.google.walkaround.wave.server.auth.AccountStore;
import com.google.walkaround.wave.server.auth.StableUserId;
import com.google.walkaround.wave.server.conv.ConvMetadataStore;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

/**
 * @author danilatos@google.com (Daniel Danilatos)
 */
public class WaveIndexer {

  private static final Logger log = Logger.getLogger(WaveIndexer.class.getName());

  // Unavailable index prefixes:
  // "USRIDX-".
  // "USRIDX2-".
  // "USRIDX3-".
  private static final String USER_WAVE_INDEX_PREFIX = "USRIDX4-";

  // Unavailable field names:
  // "blips"
  // "modified"
  private static final String UNREAD_BLIP_COUNT_FIELD = "unread";
  private static final String BLIP_COUNT_FIELD = "blips2";
  private static final String IN_FOLDER_FIELD = "in";
  private static final String CREATOR_FIELD = "creator";
  private static final String TITLE_FIELD = "title";
  private static final String CONTENT_FIELD = "content";
  // We use minutes since epoch because numeric fields can be at most 2.14...E9.
  private static final String MODIFIED_MINUTES_FIELD = "modified2";

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
    String indexableText;
    String title;
    String creator;
    long lastModifiedMillis;
    int version;
    WaveletId waveletId;
    Conversation model;
    int blipCount;
  }

  private final WaveSerializer serializer;
  private final IndexManager indexManager;
  private final CheckedDatastore datastore;
  private final MutationLogFactory convStore;
  private final MutationLogFactory udwStore;
  private final UserDataWaveletDirectory udwDirectory;
  private final AccountStore accountStore;
  private final RandomBase64Generator random;
  private final ConvMetadataStore convMetadataStore;

  @Inject
  public WaveIndexer(CheckedDatastore datastore,
      @ConvStore MutationLogFactory convStore,
      @UdwStore MutationLogFactory udwStore,
      UserDataWaveletDirectory udwDirectory,
      IndexManager indexManager,
      AccountStore users,
      RandomBase64Generator random,
      ConvMetadataStore convMetadataStore) {
    this.datastore = datastore;
    this.convStore = convStore;
    this.udwStore = udwStore;
    this.udwDirectory = udwDirectory;
    this.indexManager = indexManager;
    this.accountStore = users;
    this.random = random;
    this.convMetadataStore = convMetadataStore;
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
  public void indexConversation(SlobId convId) throws RetryableFailure, PermanentFailure, 
      WaveletLockedException {
    // TODO(danilatos): Handle waves for participants that have been removed.
    // Currently they will remain in the inbox but they won't have access until
    // we implement snapshotting of the waves at the point the participants
    // were removed (or similar).
    log.info("Indexing conversation for all participants");
    ConvFields convFields = getConvFields(convId);

    List<ConvUdwMapping> convUserMappings = udwDirectory.getAllUdwIds(convId);
    Map<ParticipantId, SlobId> participantsToUdws = Maps.newHashMap();
    for (ConvUdwMapping m : convUserMappings) {
      participantsToUdws.put(accountStore.get(m.getKey().getUserId()).getParticipantId(), m.getUdwId());
    }

    log.info("Participants: " + convFields.model.getParticipantIds().size()
        + ", UDWs: " + participantsToUdws.size());

    for (ParticipantId participant : convFields.model.getParticipantIds()) {
      SlobId udwId = participantsToUdws.get(participant);
      log.info("Indexing " + convId.getId() + " for " + participant.getAddress() +
          (udwId != null ? " with udw " + udwId : " with no udw"));

      Supplement supplement = udwId == null ? null : getSupplement(loadUdw(convId, udwId));
      index(convFields, participant, supplement);
    }
  }

  // "possibly" removed because they could have been re-added and we need
  // to deal with that possible race condition.
  public void unindexConversationForPossiblyRemovedParticipants(SlobId convId,
      Set<ParticipantId> possiblyRemovedParticipants)
          throws RetryableFailure, PermanentFailure, WaveletLockedException {
    for (ParticipantId removed : possiblyRemovedParticipants) {
      log.info("Unindexing " + convId + " for " + removed);
      getIndex(removed).remove(convId.getId());
    }
    // As an easy way to avoid the race condition, we re-index the whole conv slob
    // strictly after removing.
    indexConversation(convId);
  }

  /**
   * Indexes the wave only for the user of the given user data wavelet
   */
  public void indexSupplement(SlobId udwId)
      throws PermanentFailure, RetryableFailure, WaveletLockedException {
    log.info("Indexing conversation for participant with udwId " + udwId);
    StateAndVersion raw;
    SlobId convId;
    StableUserId udwOwner;
    CheckedTransaction tx = datastore.beginTransaction();
    try {
      MutationLog l = udwStore.create(tx, udwId);
      ObsoleteWaveletMetadataGsonImpl metadata;
      String metadataString = l.getMetadata();
      try {
        metadata = GsonProto.fromGson(new ObsoleteWaveletMetadataGsonImpl(), metadataString);
      } catch (MessageException e) {
        throw new RuntimeException("Failed to parse obsolete metadata: " + metadataString);
      }
      Assert.check(metadata.hasUdwMetadata(), "Metadata not udw: %s, %s", udwId, metadataString);
      convId = new SlobId(metadata.getUdwMetadata().getAssociatedConvId());
      udwOwner = new StableUserId(metadata.getUdwMetadata().getOwner());
      raw = l.reconstruct(null);
    } finally {
      tx.rollback();
    }
    WaveletName waveletName = IdHack.udwWaveletNameFromConvObjectIdAndUdwObjectId(convId, udwId);
    // TODO(ohler): avoid serialization/deserialization here
    WaveletDataImpl waveletData = deserializeWavelet(waveletName, raw.getState().snapshot());
    Assert.check(raw.getVersion() == waveletData.getVersion(),
        "Raw version %s does not match wavelet version %s",
        raw.getVersion(), waveletData.getVersion());
    PrimitiveSupplement supplement = getPrimitiveSupplement(waveletData);
    ConvFields convFields = getConvFields(convId);
    ParticipantId participantId = accountStore.get(udwOwner).getParticipantId();
    if (!convFields.model.getParticipantIds().contains(participantId)) {
      log.info(participantId + " is not currently a participant on " + convId
          + ", not indexing");
      return;
    }
    index(convFields, participantId, new SupplementImpl(supplement));
  }

  private ConvFields getConvFields(SlobId convId) throws PermanentFailure, RetryableFailure,
      WaveletLockedException {
    ConvFields fields = new ConvFields();
    WaveletDataImpl convData = loadConv(convId);

    fields.slobId = convId;
    fields.title = Util.extractTitle(convData);
    fields.creator = convData.getCreator().getAddress();
    fields.lastModifiedMillis = convData.getLastModifiedTime();
    fields.version = (int) convData.getVersion();
    fields.waveletId = convData.getWaveletId();
    fields.model = getConversation(convData);
    fields.indexableText = TextRenderer.renderToText(fields.model);
    fields.blipCount = countBlips(fields.model);

    return fields;
  }

  private String shortenTextMaybe(String in) {
    if (in.length() <= FieldChecker.MAXIMUM_TEXT_LENGTH) {
      return in;
    }
    // Not sure what the best strategy is; for now, remove duplicate tokens.
    String[] tokens = in.split("\\s", -1);
    Set<String> noDups = Sets.newLinkedHashSet(Arrays.asList(tokens));
    StringBuilder b = new StringBuilder();
    for (String s : noDups) {
      b.append(s + "\n");
    }
    String out = b.toString();
    if (out.length() <= FieldChecker.MAXIMUM_TEXT_LENGTH) {
      log.info("Shortened " + in.length() + " to " + out.length());
      return out;
    } else {
      log.warning("Shortened " + in.length() + " to " + out.length()
          + "; still too long, truncating");
      return out.substring(0, FieldChecker.MAXIMUM_TEXT_LENGTH);
    }
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
    builder.addField(Field.newBuilder().setName(CONTENT_FIELD).setText(
        shortenTextMaybe(conv.indexableText)));
    builder.addField(Field.newBuilder().setName(TITLE_FIELD).setText(
        shortenTextMaybe(conv.title)));
    builder.addField(Field.newBuilder().setName(CREATOR_FIELD).setText(conv.creator));
    builder.addField(Field.newBuilder().setName(MODIFIED_MINUTES_FIELD).setNumber(
        conv.lastModifiedMillis / 1000 / 60));
    builder.addField(Field.newBuilder().setName(IN_FOLDER_FIELD).setText("inbox"));
    builder.addField(Field.newBuilder().setName(BLIP_COUNT_FIELD).setNumber(conv.blipCount));
    builder.addField(Field.newBuilder().setName(UNREAD_BLIP_COUNT_FIELD).setText(
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

  private WaveletDataImpl loadConv(SlobId id)
      throws PermanentFailure, RetryableFailure, WaveletLockedException {
    StateAndVersion raw;
    CheckedTransaction tx = datastore.beginTransaction();
    try {
      ConvMetadata metadata = convMetadataStore.get(tx, id);
      if (metadata.hasImportMetadata()
          && !metadata.getImportMetadata().getImportFinished()) {
        throw new WaveletLockedException("Conv " + id + " locked: " + metadata);
      }
      MutationLog l = convStore.create(tx, id);
      raw = l.reconstruct(null);
    } finally {
      tx.rollback();
    }
    WaveletName waveletName = IdHack.convWaveletNameFromConvObjectId(id);
    // TODO(ohler): avoid serialization/deserialization here
    WaveletDataImpl waveletData = deserializeWavelet(waveletName,
        raw.getState().snapshot());
    Assert.check(raw.getVersion() == waveletData.getVersion(),
        "Raw version %s does not match wavelet version %s",
        raw.getVersion(), waveletData.getVersion());
    return waveletData;
  }

  private WaveletDataImpl loadUdw(SlobId convId, SlobId udwId)
      throws PermanentFailure, RetryableFailure {
    StateAndVersion raw;
    CheckedTransaction tx = datastore.beginTransaction();
    try {
      MutationLog l = udwStore.create(tx, udwId);
      raw = l.reconstruct(null);
    } finally {
      tx.rollback();
    }
    WaveletName waveletName = IdHack.udwWaveletNameFromConvObjectIdAndUdwObjectId(convId, udwId);
    // TODO(ohler): avoid serialization/deserialization here
    WaveletDataImpl waveletData = deserializeWavelet(waveletName,
        raw.getState().snapshot());
    Assert.check(raw.getVersion() == waveletData.getVersion(),
        "Raw version %s does not match wavelet version %s",
        raw.getVersion(), waveletData.getVersion());
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
    // TODO(ohler): Avoid these deprecated classes.  However, the new API isn't quite ready
    // (specifically, SortDirection is package-private), so we can't yet.
    SearchRequest request;
    try {
      request = SearchRequest.newBuilder()
          .setQuery(query)
          .setFieldsToSnippet(CONTENT_FIELD)
          .setOffset(offset)
          .setLimit(limit)
          .addSortSpec(SortSpec.newBuilder()
              .setType(SortSpec.SortType.CUSTOM)
              .setExpression(MODIFIED_MINUTES_FIELD)
              .setDirection(SortDirection.DESCENDING)
              .setDefaultValueNumeric(0))
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
        unreadCount = getRequiredStringField(doc, UNREAD_BLIP_COUNT_FIELD);
      }
      entries.add(new UserIndexEntry(
          new SlobId(doc.getId()),
          ParticipantId.ofUnsafe(
              HACK ? "hack@hack.com" : getRequiredStringField(doc, CREATOR_FIELD)),
          HACK ? "XXX" : getRequiredStringField(doc, TITLE_FIELD),
          snippetHtml,
          HACK ? 1L : getRequiredLongField(doc, MODIFIED_MINUTES_FIELD)
              * 60 * 1000,
          Ints.checkedCast(HACK ? 23 : getRequiredLongField(doc, BLIP_COUNT_FIELD)),
          "no".equals(unreadCount) ? null : Integer.parseInt(unreadCount)
          ));
    }
    return entries;
  }

  @Nullable private Field getOptionalUniqueField(Document doc, String name) {
    Iterable<Field> fields = doc.getField(name);
    if (fields == null) {
      return null;
    }
    List<Field> list = ImmutableList.copyOf(fields);
    if (list.isEmpty()) {
      return null;
    }
    Preconditions.checkArgument(list.size() == 1, "%s: Field %s not unique, found %s: %s",
        doc, name, list.size(), list);
    return Iterables.getOnlyElement(list);
  }

  private Field getRequiredUniqueField(Document doc, String name) {
    Field field = getOptionalUniqueField(doc, name);
    Preconditions.checkArgument(field != null, "%s: No field %s", doc, name);
    return field;
  }

  private String getRequiredStringField(Document doc, String name) {
    return getRequiredUniqueField(doc, name).getText();
  }

  private long getRequiredLongField(Document doc, String name) {
    return (long) getRequiredUniqueField(doc, name).getNumber().doubleValue();
  }

  private String describe(Document doc) {
    return "doc[" + doc.getId() + " with " + doc.getFields() + "]";
  }
}
