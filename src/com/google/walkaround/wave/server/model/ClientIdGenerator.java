package com.google.walkaround.wave.server.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.waveprotocol.wave.model.wave.ParticipantId;

import com.google.inject.Inject;
import com.google.walkaround.slob.shared.ClientId;
import com.google.walkaround.util.shared.RandomBase64Generator;

/**
 * Generates {@link ClientId}s.
 *
 * <p>{@code ClientId}s are used to reject multiple concurrent submits from the
 * same client, so we assign a random one to each web client, and a fixed one to
 * each robot based on its robot id.
 */
public class ClientIdGenerator {

  private final RandomBase64Generator random64;

  @Inject public ClientIdGenerator(RandomBase64Generator random64) {
    this.random64 = checkNotNull(random64, "Null random64");
  }

  public ClientId getIdForCreation() {
    return new ClientId("creation");
  }

  public ClientId getIdForRobot(ParticipantId robotId) {
    return new ClientId("robot-" + robotId.getAddress());
  }

  public ClientId getIdForNewWebClient() {
    return new ClientId("web-"
        + random64.next(
            // TODO(ohler): justify this number
            8));
  }

}
