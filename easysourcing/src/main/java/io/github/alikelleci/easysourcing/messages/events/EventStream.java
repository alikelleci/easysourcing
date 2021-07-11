package io.github.alikelleci.easysourcing.messages.events;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
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
public class EventStream {

  private final Set<String> topics;
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;

  public EventStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.eventHandlers = eventHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // Stores
    StoreBuilder storeBuilder1 = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("event-redirects"), Serdes.String(), Serdes.Long())
        .withLoggingEnabled(Collections.emptyMap());

    builder.addStateStore(storeBuilder1);

    if (OPERATION_MODE == OperationMode.RETRY) {
      topics.clear();
      topics.add(APPLICATION_ID.concat(".events-retry"));
    }

    // --> Events
    KStream<String, JsonNode> events = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null);

    // Events --> Results
    KStream<String, Object>[] results = events
        .transform(() -> new EventTransformer(eventHandlers), "event-redirects")
        .branch(
            (key, value) -> value == null, // processed
            (key, value) -> value != null  // not processed
        );

    // Publish unprocessed records to retry topic
    results[1]
        .to((key, result, recordContext) -> APPLICATION_ID.concat(".events-retry"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}
