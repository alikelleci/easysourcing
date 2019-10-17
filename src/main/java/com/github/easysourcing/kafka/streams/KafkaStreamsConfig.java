package com.github.easysourcing.kafka.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
//@EnableKafkaStreams
@Configuration
public class KafkaStreamsConfig {

  @Value(" ${spring.kafka.bootstrap-servers}")
  private String BOOTSTRAP_SERVERS;

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;


  //    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  @Bean
  public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);


    return new KafkaStreamsConfiguration(properties);
  }


  //    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
  @Bean
  public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaStreamsConfiguration kafkaStreamsConfiguration) {
    StreamsBuilderFactoryBean builder = new StreamsBuilderFactoryBean(kafkaStreamsConfiguration);
    builder.setStateListener((newState, oldState) -> log.warn("State changed from {} to {}", oldState, newState));
    builder.setUncaughtExceptionHandler((t, e) -> log.error("Exception handler triggered ", e));

    return builder;
  }


}
