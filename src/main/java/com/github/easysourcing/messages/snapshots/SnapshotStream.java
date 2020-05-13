package com.github.easysourcing.messages.snapshots;


import com.github.easysourcing.messages.Message;
import com.github.easysourcing.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class SnapshotStream {

  private final Set<String> topics;
  private final ConcurrentMap<Class<?>, SnapshotHandler> snapshotHandlers;
  private final boolean frequentCommits;

  public SnapshotStream(Set<String> topics, ConcurrentMap<Class<?>, SnapshotHandler> snapshotHandlers, boolean frequentCommits) {
    this.topics = topics;
    this.snapshotHandlers = snapshotHandlers;
    this.frequentCommits = frequentCommits;
  }

  public KStream<String, Snapshot> buildStream(StreamsBuilder builder) {
    // --> Snapshots
    KStream<String, Snapshot> snapshotKStream = builder.stream(topics,
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Message.class).noTypeInfo()))
        .filter((key, message) -> key != null)
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getAggregateId() != null)
        .filter((key, message) -> message.getPayload() != null)
        .filter((key, message) -> message.getTopicInfo() != null)
        .filter((key, message) -> message instanceof Snapshot)
        .mapValues((key, message) -> (Snapshot) message);

    // Snapshots --> Void
    snapshotKStream
        .transformValues(() -> new SnapshotTransformer(snapshotHandlers, frequentCommits));

    return snapshotKStream;
  }

}
