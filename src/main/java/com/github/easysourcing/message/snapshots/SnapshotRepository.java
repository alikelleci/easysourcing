package com.github.easysourcing.message.snapshots;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class SnapshotRepository {

  @Autowired
  private StreamsBuilderFactoryBean streamsBuilderFactoryBean;


  public <T> T get(String id) {
    ReadOnlyKeyValueStore<String, Snapshot<T>> keyValueStore = streamsBuilderFactoryBean.getKafkaStreams().store("snapshots-store", QueryableStoreTypes.keyValueStore());
    Snapshot<T> snapshot = keyValueStore.get(id);
    if (snapshot != null) {
      return snapshot.getPayload();
    }
    return null;
  }

}
