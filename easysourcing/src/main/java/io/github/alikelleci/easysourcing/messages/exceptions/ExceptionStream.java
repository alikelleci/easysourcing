package io.github.alikelleci.easysourcing.messages.exceptions;


import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.MessageTransformer;
import io.github.alikelleci.easysourcing.messages.commands.Command;
import io.github.alikelleci.easysourcing.messages.upcasters.PayloadTransformer;
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
public class ExceptionStream {

  private final Set<String> topics;
  private final MultiValuedMap<String, Upcaster> upcasters;
  private final MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers;

  public ExceptionStream(Set<String> topics, MultiValuedMap<String, Upcaster> upcasters, MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers) {
    this.topics = topics;
    this.upcasters = upcasters;
    this.exceptionHandlers = exceptionHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Failed commands --> Void
    builder.stream(topics, Consumed.with(Serdes.String(), CustomSerdes.Json(JsonNode.class)))
        // Filter
        .filter((key, value) -> key != null)
        .filter((key, value) -> value != null)

        // Upcast & convert
        .transformValues(() -> new PayloadTransformer(upcasters))
        .transformValues(() -> new MessageTransformer<>(Command.class))

        // Filter
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null)

        // Invoke handlers
        .transformValues(() -> new ExceptionTransformer(exceptionHandlers));
  }

}
