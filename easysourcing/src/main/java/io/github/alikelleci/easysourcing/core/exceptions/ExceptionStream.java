package io.github.alikelleci.easysourcing.core.exceptions;


import io.github.alikelleci.easysourcing.core.commands.Command;
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
  private final MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers;

  public ExceptionStream(Set<String> topics, MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers) {
    this.topics = topics;
    this.exceptionHandlers = exceptionHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Failed commands
    KStream<String, Command> failedCommands  = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // Failed commands --> Void
    failedCommands
        .transformValues(() -> new ExceptionTransformer(exceptionHandlers));
  }

}
