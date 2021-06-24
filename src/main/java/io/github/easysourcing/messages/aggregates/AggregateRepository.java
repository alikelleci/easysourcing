package io.github.easysourcing.messages.aggregates;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class AggregateRepository {

  private final KafkaStreams kafkaStreams;

  public AggregateRepository(KafkaStreams kafkaStreams) {
    this.kafkaStreams = kafkaStreams;
  }

  public <T> T get(String id) {
    ReadOnlyKeyValueStore<String, Aggregate> keyValueStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType("snapshot-store", QueryableStoreTypes.keyValueStore()));
    Aggregate snapshot = keyValueStore.get(id);
    if (snapshot != null) {
      return (T) snapshot.getPayload();
    }
    return null;
  }

}
