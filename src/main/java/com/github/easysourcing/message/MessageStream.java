package com.github.easysourcing.message;


import com.github.easysourcing.message.aggregates.Aggregate;
import com.github.easysourcing.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Collections;
import java.util.Set;

@Slf4j
public class MessageStream {

  private final Set<String> topics;

  public MessageStream(Set<String> topics) {
    this.topics = topics;
  }

  public KStream<String, Message> buildStream(StreamsBuilder builder) {
    // Snapshot store
    builder.addStateStore(
        Stores.timestampedKeyValueStoreBuilder(
            Stores.persistentTimestampedKeyValueStore("snapshot-store"),
            Serdes.String(),
            new JsonSerde<>(Aggregate.class).noTypeInfo()
        ).withLoggingEnabled(Collections.singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG, "604800000")) // 7 days
    );

    // --> Messages
    return builder.stream(topics,
        Consumed.with(Serdes.String(), new CustomJsonSerde<>(Message.class).noTypeInfo()))
        .filter((key, event) -> key != null)
        .filter((key, event) -> event != null)
        .filter((key, event) -> event.getId() != null)
        .filter((key, event) -> event.getTopicInfo() != null)
        .filter((key, event) -> event.getPayload() != null);
  }

}
