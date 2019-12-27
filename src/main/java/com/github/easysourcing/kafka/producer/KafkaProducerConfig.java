package com.github.easysourcing.kafka.producer;

import com.github.easysourcing.message.Message;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

  @Value(" ${easysourcing.bootstrap-servers}")
  private String BOOTSTRAP_SERVERS;

  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");

    return properties;
  }

  @Bean
  public DefaultKafkaProducerFactory producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs(),
        new StringSerializer(),
        new JsonSerializer<>()
            .noTypeInfo());
  }

  @Bean
  public KafkaTemplate<String, Message> kafkaTemplate(DefaultKafkaProducerFactory<String, Message> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

}
