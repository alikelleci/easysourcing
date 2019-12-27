package com.github.easysourcing.kafka.streams;

import com.github.easysourcing.message.aggregates.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafkaStreams
@Configuration
public class KafkaStreamsConfig {

  @Value(" ${easysourcing.bootstrap-servers}")
  private String BOOTSTRAP_SERVERS;

  @Value("${easysourcing.application-id}")
  private String APPLICATION_ID;

  @Value("${easysourcing.replication-factor:1}")
  private int REPLICATION_FACTOR;


  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, REPLICATION_FACTOR);
    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
//    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
//    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

    return new KafkaStreamsConfiguration(properties);
  }


  @Bean
  public StreamsBuilderFactoryBeanCustomizer customizer() {
    return fb -> {
      fb.setStateListener((newState, oldState) -> log.warn("State changed from {} to {}", oldState, newState));
      fb.setUncaughtExceptionHandler((t, e) -> log.error("Exception handler triggered ", e));
    };
  }

  @Bean
  public StreamsBuilder builder(StreamsBuilder builder) {
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshots"), Serdes.String(), new JsonSerde<>(Aggregate.class).noTypeInfo())
        .withLoggingEnabled(Collections.singletonMap(TopicConfig.DELETE_RETENTION_MS_CONFIG, "604800000"))); // 7 days

    return builder;
  }
}
