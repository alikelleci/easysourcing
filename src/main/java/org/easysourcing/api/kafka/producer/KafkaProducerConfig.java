package org.easysourcing.api.kafka.producer;

import org.easysourcing.api.message.Message;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

  @Value(" ${spring.kafka.bootstrap-servers}")
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
  public ProducerFactory<String, Message> producerFactory() {
    JsonSerializer<Message> jsonSerializer = new JsonSerializer<>();
    jsonSerializer.setAddTypeInfo(false);

    return new DefaultKafkaProducerFactory<>(producerConfigs(), new StringSerializer(), jsonSerializer);
  }

  @Bean
  public KafkaTemplate<String, Message> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }


}
