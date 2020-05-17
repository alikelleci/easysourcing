package com.github.easysourcing.messages.results;


import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

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
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Message.class).noTypeInfo()))
        .filter((key, message) -> key != null)
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getPayload() != null)
        .filter((key, message) -> message.getTopicInfo() != null)
        .filter((key, message) -> message.getAggregateId() != null)
        .filter((key, message) -> message instanceof Command)
        .mapValues((key, message) -> (Command) message);

    // Results --> Void
    resultKStream
        .transformValues(() -> new ResultTransformer(resultHandlers, frequentCommits));
  }

}
