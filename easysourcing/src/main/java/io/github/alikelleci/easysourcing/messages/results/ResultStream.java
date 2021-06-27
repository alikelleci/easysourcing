package io.github.alikelleci.easysourcing.messages.results;


import io.github.alikelleci.easysourcing.messages.commands.Command;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Set;

@Slf4j
public class ResultStream {

  private final Set<String> topics;
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers;

  public ResultStream(Set<String> topics, MultiValuedMap<Class<?>, ResultHandler> resultHandlers) {
    this.topics = topics;
    this.resultHandlers = resultHandlers;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Results
    KStream<String, Command> resultKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // Results --> Void
    resultKStream
        .transformValues(() -> new ResultTransformer(resultHandlers));
  }

}
