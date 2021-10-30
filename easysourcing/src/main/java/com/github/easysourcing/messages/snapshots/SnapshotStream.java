package com.github.easysourcing.messages.snapshots;


import com.github.easysourcing.constants.Topics;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

@Slf4j
public class SnapshotStream {

  public void buildStream(StreamsBuilder builder) {
    // --> Snapshots
    KStream<String, Aggregate> snapshotKStream = builder.stream(Topics.SNAPSHOTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)))
        .filter((key, aggregate) -> key != null)
        .filter((key, aggregate) -> aggregate != null)
        .filter((key, aggregate) -> aggregate.getPayload() != null)
        .filter((key, aggregate) -> aggregate.getTopicInfo() != null)
        .filter((key, aggregate) -> aggregate.getAggregateId() != null);

    // Snapshots --> Void
    snapshotKStream
        .transformValues(SnapshotTransformer::new);
  }

}
