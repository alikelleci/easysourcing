package io.github.alikelleci.easysourcing.messages.errors;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
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
public class ErrorStream {

  private final Set<String> topics;
  private final MultiValuedMap<Class<?>, ErrorHandler> errorHandlers;

  public ErrorStream(Set<String> topics, MultiValuedMap<Class<?>, ErrorHandler> errorHandlers) {
    this.topics = topics;
    this.errorHandlers = errorHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // Stores
    StoreBuilder storeBuilder1 = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("error-redirects"), Serdes.String(), Serdes.Long())
        .withLoggingEnabled(Collections.emptyMap());

    builder.addStateStore(storeBuilder1);

    if (OPERATION_MODE == OperationMode.RETRY) {
      topics.clear();
      topics.add( APPLICATION_ID.concat(".errors-retry"));
    }

    // --> Error commands
    KStream<String, JsonNode> errors = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Errors --> Results
    KStream<String, Object>[] results = errors
        .transform(() -> new ErrorTransformer(errorHandlers), "error-redirects")
        .branch(
            (key, value) -> value == null, // processed
            (key, value) -> value != null  // not processed
        );

    // Publish unprocessed records to retry topic
    results[1]
        .to((key, result, recordContext) -> APPLICATION_ID.concat(".errors-retry"),
            Produced.with(Serdes.String(), CustomSerdes.Json(Object.class)));
  }

}
