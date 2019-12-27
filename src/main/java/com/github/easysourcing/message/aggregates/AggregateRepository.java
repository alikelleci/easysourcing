package com.github.easysourcing.message.aggregates;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Repository;

@Repository
public class AggregateRepository {

  @Autowired
  private StreamsBuilderFactoryBean streamsBuilderFactoryBean;


  public <T> T get(String id) {
    ReadOnlyKeyValueStore<String, Aggregate<T>> keyValueStore = streamsBuilderFactoryBean.getKafkaStreams().store("snapshots", QueryableStoreTypes.keyValueStore());
    Aggregate<T> snapshot = keyValueStore.get(id);
    if (snapshot != null) {
      return snapshot.getPayload();
    }
    return null;
  }

}
