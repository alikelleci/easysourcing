package io.github.alikelleci.easysourcing.messages.snapshots;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.Result;
import io.github.alikelleci.easysourcing.messages.events.EventTransformer;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Set;

import static io.github.alikelleci.easysourcing.EasySourcingBuilder.APPLICATION_ID;
import static io.github.alikelleci.easysourcing.EasySourcingBuilder.OPERATION_MODE;

@Slf4j
public class SnapshotStream {

  private final Set<String> topics;
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers;

  public SnapshotStream(Set<String> topics, MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers) {
    this.topics = topics;
    this.snapshotHandlers = snapshotHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // Stores
    StoreBuilder storeBuilder1 = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshot-redirects"), Serdes.String(), Serdes.Long())
        .withLoggingEnabled(Collections.emptyMap());

    builder.addStateStore(storeBuilder1);

    if (OPERATION_MODE == OperationMode.RETRY) {
      topics.clear();
      topics.add( APPLICATION_ID.concat(".snapshots-retry"));
    }

    // --> Snapshots
    KStream<String, JsonNode> snapshots = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, snapshot) -> key != null)
        .filter((key, snapshot) -> snapshot != null);

    // -->  Snapshots
    snapshots
        .transform(() -> new SnapshotTransformer(snapshotHandlers), "snapshot-redirects")
        .filter((key, result) -> result instanceof Result.Unprocessed)
        .mapValues((key, result) -> result.getPayload())
        .to((key, result, recordContext) -> APPLICATION_ID.concat(".snapshots-retry"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}
