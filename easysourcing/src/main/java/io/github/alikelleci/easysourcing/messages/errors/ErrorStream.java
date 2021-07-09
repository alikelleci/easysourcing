package io.github.alikelleci.easysourcing.messages.errors;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Set;

@Slf4j
public class ErrorStream {

  private final Set<String> topics;
  private final MultiValuedMap<Class<?>, ErrorHandler> errorHandlers;

  public ErrorStream(Set<String> topics, MultiValuedMap<Class<?>, ErrorHandler> errorHandlers) {
    this.topics = topics;
    this.errorHandlers = errorHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Failed commands
    KStream<String, JsonNode> failedCommands = builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);

    // Failed commands --> Void
    failedCommands
        .transformValues(() -> new ErrorTransformer(errorHandlers));
  }

}
