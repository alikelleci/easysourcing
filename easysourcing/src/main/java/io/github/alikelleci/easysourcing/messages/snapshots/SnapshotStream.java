package io.github.alikelleci.easysourcing.messages.snapshots;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.MessageTransformer;
import io.github.alikelleci.easysourcing.messages.upcasters.PayloadTransformer;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Set;

@Slf4j
public class SnapshotStream {

  private final Set<String> topics;
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers;

  public SnapshotStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.snapshotHandlers = snapshotHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Snapshots --> Void
    builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        // Filter
        .filter((key, value) -> key != null)
        .filter((key, value) -> value != null)

        // Upcast & convert
        .transformValues(() -> new PayloadTransformer(upcasters))
        .transformValues(() -> new MessageTransformer<>(Snapshot.class))

        // Filter
        .filter((key, snapshot) -> snapshot != null)
        .filter((key, snapshot) -> snapshot.getPayload() != null)
        .filter((key, snapshot) -> snapshot.getTopicInfo() != null)
        .filter((key, snapshot) -> snapshot.getAggregateId() != null)

        // Invoke handlers
        .transformValues(() -> new SnapshotTransformer(snapshotHandlers));
  }

}
