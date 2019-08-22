package com.easysourcing.api.message.snapshots;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class SnapshotRepository {

  @Autowired
  private StreamsBuilderFactoryBean streamsBuilderFactoryBean;


  public <T extends Snapshotable> T get(String id, Class<T> tClass) {
    ReadOnlyKeyValueStore<String, T> keyValueStore = streamsBuilderFactoryBean.getKafkaStreams().store("snapshots", QueryableStoreTypes.keyValueStore());
    return keyValueStore.get(id);
  }

}
