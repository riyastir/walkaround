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

import com.google.appengine.api.search.AddException;
import com.google.appengine.api.search.AddResponse;
import com.google.appengine.api.search.Consistency;
import com.google.appengine.api.search.Document;
import com.google.appengine.api.search.Field;
import com.google.appengine.api.search.Index;
import com.google.appengine.api.search.IndexSpec;
import com.google.appengine.api.search.OperationResult;
import com.google.appengine.api.search.Query;
import com.google.appengine.api.search.QueryOptions;
import com.google.appengine.api.search.Results;
import com.google.appengine.api.search.ScoredDocument;
import com.google.appengine.api.search.SearchException;
import com.google.appengine.api.search.SearchQueryException;
import com.google.appengine.api.search.SearchService;
import com.google.appengine.api.search.SortExpression;
import com.google.appengine.api.search.SortOptions;
import com.google.appengine.api.search.StatusCode;
import com.google.appengine.api.search.checkers.FieldChecker;
import com.google.appengine.api.search.checkers.IndexChecker;
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
import com.google.walkaround.wave.shared.Versions;
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
import java.lang.reflect.InvocationTargetException;
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
  private static final String USER_WAVE_INDEX_PREFIX = "USRIDX5-";

  private static final String ID_FIELD = "id";
  private static final String CREATOR_FIELD = "creator";
  private static final String TITLE_FIELD = "title";
  private static final String CONTENT_FIELD = "content";
  // We use minutes since epoch because numeric fields can be at most 2.14...E9.
  private static final String MODIFIED_MINUTES_FIELD = "modified";
  private static final String UNREAD_BLIP_COUNT_FIELD = "unread";
  private static final String BLIP_COUNT_FIELD = "blips";
  private static final String IN_FOLDER_FIELD = "in";
  private static final String IS_FIELD = "is";

  private static final String IN_INBOX_TEXT = "inbox";
  private static final String IS_UNREAD_ATOM = "unread";
  private static final String IS_READ_ATOM = "read";
  private static final String IS_ARCHIVED_ATOM = "archived";
  private static final String IS_FOLLOWED_ATOM = "followed";

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
    @Nullable Conversation conv;
    List<ParticipantId> participants;
    int blipCount;
  }

  private final WaveSerializer serializer;
  private final SearchService searchService;
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
      SearchService searchService,
      AccountStore users,
      RandomBase64Generator random,
      ConvMetadataStore convMetadataStore) {
    this.datastore = datastore;
    this.convStore = convStore;
    this.udwStore = udwStore;
    this.udwDirectory = udwDirectory;
    this.searchService = searchService;
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
    ConvFields fields = getConvFields(convId);

    List<ConvUdwMapping> convUserMappings = udwDirectory.getAllUdwIds(convId);
    Map<ParticipantId, SlobId> participantsToUdws = Maps.newHashMap();
    for (ConvUdwMapping m : convUserMappings) {
      participantsToUdws.put(accountStore.get(m.getKey().getUserId()).getParticipantId(),
          m.getUdwId());
    }

    log.info("Participants: " + fields.participants.size()
        + ", UDWs: " + participantsToUdws.size());

    for (ParticipantId participant : fields.participants) {
      SlobId udwId = participantsToUdws.get(participant);
      log.info("Indexing " + convId.getId() + " for " + participant.getAddress() +
          (udwId != null ? " with udw " + udwId : " with no udw"));

      Supplement supplement = udwId == null ? null : getSupplement(loadUdw(convId, udwId));
      index(fields, participant, supplement);
    }
  }

  // "possibly" removed because they could have been re-added and we need
  // to deal with that possible race condition.
  public void unindexConversationForPossiblyRemovedParticipants(SlobId convId,
      Set<ParticipantId> possiblyRemovedParticipants)
          throws RetryableFailure, PermanentFailure, WaveletLockedException {
    for (ParticipantId removed : possiblyRemovedParticipants) {
      if (!isParticipantValidForIndexing(removed)) {
        log.info(convId + ": Removed participant not valid for indexing: " + removed);
      } else {
        log.info(convId + ": Unindexing for " + removed);
        getIndex(removed).remove(convId.getId());
      }
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
    ConvFields fields = getConvFields(convId);
    ParticipantId participantId = accountStore.get(udwOwner).getParticipantId();
    if (!fields.participants.contains(participantId)) {
      log.info(participantId + " is not currently a participant on " + convId
          + ", not indexing");
      return;
    }
    index(fields, participantId, new SupplementImpl(supplement));
  }

  private ConvFields getConvFields(SlobId convId) throws PermanentFailure, RetryableFailure,
      WaveletLockedException {
    ConvFields fields = new ConvFields();
    WaveletDataImpl convData = loadConv(convId);
    fields.slobId = convId;
    fields.title = Util.extractTitle(convData);
    fields.creator = convData.getCreator().getAddress();
    fields.lastModifiedMillis = convData.getLastModifiedTime();
    fields.version = Versions.truncate(convData.getVersion());
    fields.waveletId = convData.getWaveletId();
    fields.conv = getConversation(convData);
    fields.participants = ImmutableList.copyOf(convData.getParticipants());
    fields.indexableText = fields.conv == null ? "" : TextRenderer.renderToText(fields.conv);
    fields.blipCount = fields.conv == null ? 0 : countBlips(fields.conv);
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

  private boolean isParticipantValidForIndexing(ParticipantId user) {
    String indexName = getIndexName(user);
    try {
      IndexChecker.checkName(indexName);
    } catch (Exception e) {
      log.log(Level.INFO, "Participant not valid for indexing: " + user, e);
      return false;
    }
    return true;
  }

  private void index(ConvFields fields, ParticipantId user, @Nullable Supplement supplement)
      throws RetryableFailure, PermanentFailure {
    if (!isParticipantValidForIndexing(user)) {
      log.warning(fields.slobId + ": Participant not valid for indexing: " + user);
      return;
    }
    boolean isArchived = false;
    boolean isFollowed = true;
    int unreadBlips = fields.blipCount;
    boolean participantsUnread = true;
    if (supplement != null) {
      isArchived = supplement.isArchived(fields.waveletId, fields.version);
      isFollowed = supplement.isFollowed(true);
      unreadBlips = fields.conv == null ? 0
          : countUnreadBlips(fields.conv, fields.waveletId, fields.version, supplement);
      participantsUnread = isParticipantListUnread(fields.version, supplement);
    }

    log.info("Unread blips: " + unreadBlips + "; participants unread: " + participantsUnread
        + "; has supplement: " + (supplement != null));

    Document.Builder builder = Document.newBuilder();
    builder.setId(fields.slobId.getId());
    builder.addField(Field.newBuilder().setName(ID_FIELD).setAtom(fields.slobId.getId()));
    builder.addField(Field.newBuilder().setName(CREATOR_FIELD).setText(fields.creator));
    builder.addField(Field.newBuilder().setName(TITLE_FIELD).setText(
        shortenTextMaybe(fields.title)));
    builder.addField(Field.newBuilder().setName(CONTENT_FIELD).setText(
        shortenTextMaybe(fields.indexableText)));
    builder.addField(Field.newBuilder().setName(MODIFIED_MINUTES_FIELD).setNumber(
        fields.lastModifiedMillis / 1000 / 60));
    builder.addField(Field.newBuilder().setName(BLIP_COUNT_FIELD).setNumber(fields.blipCount));
    builder.addField(Field.newBuilder().setName(UNREAD_BLIP_COUNT_FIELD).setNumber(unreadBlips));
    if (!isArchived) {
      builder.addField(Field.newBuilder().setName(IN_FOLDER_FIELD).setText(IN_INBOX_TEXT));
    }
    builder.addField(Field.newBuilder().setName(IS_FIELD).setAtom(
        unreadBlips == 0 && !participantsUnread ? IS_READ_ATOM : IS_UNREAD_ATOM));
    if (isArchived) {
      builder.addField(Field.newBuilder().setName(IS_FIELD).setAtom(IS_ARCHIVED_ATOM));
    }
    if (isFollowed) {
      builder.addField(Field.newBuilder().setName(IS_FIELD).setAtom(IS_FOLLOWED_ATOM));
    }
    Document doc = builder.build();

    Index idx = getIndex(user);

    log.info("Saving index document  " + describe(doc));

    // TODO(danilatos): Factor out all the error handling?
    AddResponse resp;
    try {
      resp = idx.add(doc);
    } catch (AddException e) {
      throw new RetryableFailure("Error indexing " + fields.slobId, e);
    }
    for (OperationResult result : resp) {
      if (!result.getCode().equals(StatusCode.OK)) {
        throw new RetryableFailure("Error indexing " + fields.slobId + ", " + result.getMessage());
      }
    }
  }

  private int countBlips(Conversation conv) {
    return Iterables.size(BlipIterators.breadthFirst(conv));
  }

  private int countUnreadBlips(Conversation conv, WaveletId convId, int convVersion,
      Supplement supplement) {
    log.info("Seen wavelets: " + supplement.getSeenWavelets());
    log.info("Conv wavelet: " + convId);
    int unreadBlips = 0;
    for (ConversationBlip blip : BlipIterators.breadthFirst(conv)) {
      if (supplement.isBlipUnread(
              convId, blip.getId(), Versions.truncate(blip.getLastModifiedVersion()))) {
        unreadBlips++;
      }
    }
    return unreadBlips;
  }

  private boolean isParticipantListUnread(int convVersion, Supplement supplement) {
//  TODO(danilatos): Get this bit to work...  probably something wrong with the version.
//  if (supplement.isParticipantsUnread(convId, convVersion)) {
//    return 0;
//  }
    return false;
  }

  private static final WaveletFactory<OpBasedWavelet> STUB_FACTORY =
    new WaveletFactory<OpBasedWavelet>() {
        @Override public OpBasedWavelet create(WaveId waveId, WaveletId id, ParticipantId creator) {
          throw new UnsupportedOperationException();
        }
    };

  @Nullable private Conversation getConversation(WaveletDataImpl convData) {
    IdGenerator idGenerator = new IdHack.MinimalIdGenerator(
        convData.getWaveletId(),
        IdHack.FAKE_WAVELET_NAME.waveletId,
        random);
    WaveViewImpl<OpBasedWavelet> waveView = WaveViewImpl.create(STUB_FACTORY,
        convData.getWaveId(), idGenerator,
        ParticipantId.ofUnsafe("fake@fake.com"),
        WaveletConfigurator.ERROR);
    waveView.addWavelet(OpBasedWavelet.createReadOnly(convData));
    WaveBasedConversationView convView;
    try {
      convView = WaveBasedConversationView.create(waveView, idGenerator);
    } catch (Exception e) {
      log.log(Level.WARNING, convData + ": Failed to create conversation view", e);
      return null;
    }
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
    return searchService.getIndex(
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

  public List<UserIndexEntry> findWaves(ParticipantId user, String queryString, int offset, int limit)
      throws IOException {
    // Trailing whitespace causes parse exceptions...
    queryString = queryString.trim();
    log.info("Searching for " + queryString);

    SortExpression.Builder sortExpression = SortExpression.newBuilder()
        .setExpression(MODIFIED_MINUTES_FIELD)
        .setDefaultValueNumeric(0)
        .setDirection(SortExpression.SortDirection.DESCENDING);
    Query query;
    try {
      query = Query.newBuilder()
          .setOptions(QueryOptions.newBuilder()
              .setFieldsToSnippet(CONTENT_FIELD)
              .setOffset(offset)
              .setLimit(limit)
              .setSortOptions(SortOptions.newBuilder()
                  .addSortExpression(sortExpression))
              .build())
          .build(queryString);
    } catch (SearchQueryException e) {
      log.log(Level.WARNING, "Problem building query " + queryString, e);
      return Collections.emptyList();
    }

    // TODO(danilatos): Figure out why this throws unchecked exceptions for bad searches
    // but also returns status codes...?
    Results<ScoredDocument> response;
    try {
      response = getIndex(user).search(query);
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
    for (ScoredDocument result : response) {
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
      Integer unreadCount;
      if (HACK) {
        int c = new Random().nextInt(5);
        unreadCount = c == 0 ? null : c * 7;
      } else {
        int unreadBlips = getRequiredIntField(result, UNREAD_BLIP_COUNT_FIELD);
        boolean isUnread = getAtomFields(result, IS_FIELD).contains(IS_UNREAD_ATOM);
        unreadCount = unreadBlips == 0 && !isUnread ? null : unreadBlips;
      }
      entries.add(new UserIndexEntry(
          new SlobId(result.getId()),
          ParticipantId.ofUnsafe(
              HACK ? "hack@hack.com" : getRequiredStringField(result, CREATOR_FIELD)),
          HACK ? "XXX" : getRequiredStringField(result, TITLE_FIELD),
          snippetHtml,
          HACK ? 1L : getRequiredLongField(result, MODIFIED_MINUTES_FIELD)
              * 60 * 1000,
          Ints.checkedCast(HACK ? 23 : getRequiredLongField(result, BLIP_COUNT_FIELD)),
          unreadCount));
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

  private Field getRequiredUniqueField(ScoredDocument doc, String name) {
    Field field = getOptionalUniqueField(doc, name);
    Preconditions.checkArgument(field != null, "%s: No field %s", doc, name);
    return field;
  }

  private String getRequiredStringField(ScoredDocument doc, String name) {
    return getRequiredUniqueField(doc, name).getText();
  }

  private long getRequiredLongField(ScoredDocument doc, String name) {
    return (long) getRequiredUniqueField(doc, name).getNumber().doubleValue();
  }

  private int getRequiredIntField(ScoredDocument doc, String name) {
    return (int) getRequiredUniqueField(doc, name).getNumber().doubleValue();
  }

  private List<String> getAtomFields(Document doc, String name) {
    Iterable<Field> fields = doc.getField(name);
    if (fields == null) {
      return ImmutableList.of();
    }
    ImmutableList.Builder<String> out = ImmutableList.builder();
    for (Field f : fields) {
      Preconditions.checkArgument(f.getAtom() != null,
          "%s: Expected atoms for field name %s, not %s", doc, name, f);
      out.add(f.getAtom());
    }
    return out.build();
  }

  private String describe(Document doc) {
    return "doc[" + doc.getId() + " with " + doc.getFields() + "]";
  }

}
