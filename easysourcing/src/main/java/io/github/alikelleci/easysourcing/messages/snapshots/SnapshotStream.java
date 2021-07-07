package io.github.alikelleci.easysourcing.messages.snapshots;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Set;

@Slf4j
public class SnapshotStream {

  private final Set<String> topics;
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers;

  public SnapshotStream(Set<String> topics, MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers) {
    this.topics = topics;
    this.snapshotHandlers = snapshotHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Snapshots
    KStream<String, JsonNode> snapshots = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, snapshot) -> key != null)
        .filter((key, snapshot) -> snapshot != null);

    // Snapshots --> Void
    snapshots
        .transformValues(() -> new SnapshotTransformer(snapshotHandlers));
  }

}
