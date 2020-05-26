package com.github.easysourcing.messages.results;


import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class ResultStream {

  private final Set<String> topics;
  private final ConcurrentMap<Class<?>, ResultHandler> resultHandlers;
  private final boolean frequentCommits;

  public ResultStream(Set<String> topics, ConcurrentMap<Class<?>, ResultHandler> resultHandlers, boolean frequentCommits) {
    this.topics = topics;
    this.resultHandlers = resultHandlers;
    this.frequentCommits = frequentCommits;
  }

  public void buildStream(StreamsBuilder builder) {
    // --> Results
    KStream<String, Command> resultKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Command.class).noTypeInfo()))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null)
        .filter((key, command) -> command.getPayload() != null)
        .filter((key, command) -> command.getTopicInfo() != null)
        .filter((key, command) -> command.getAggregateId() != null);

    // Results --> Commands Push
    resultKStream
        .transformValues(() -> new ResultTransformer(resultHandlers, frequentCommits))
        .filter((key, commands) -> CollectionUtils.isNotEmpty(commands))
        .flatMapValues(commands -> commands)
        .filter((key, command) -> command != null)
        .map((key, command) -> KeyValue.pair(command.getAggregateId(), command))
        .to((key, command, recordContext) -> command.getTopicInfo().value(),
            Produced.with(Serdes.String(), new JsonSerde<>(Command.class).noTypeInfo()));
  }

}
