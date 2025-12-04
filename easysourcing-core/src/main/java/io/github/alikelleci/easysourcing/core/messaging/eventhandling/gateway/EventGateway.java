package io.github.alikelleci.easysourcing.core.messaging.eventhandling.gateway;

import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.support.serialization.json.util.JacksonUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Properties;


public interface EventGateway {

  void publish(Object payload, Metadata metadata, Instant timestamp);

  default void publish(Object payload, Metadata metadata) {
    publish(payload, metadata, null);
  }

  default void publish(Object payload) {
    publish(payload, null, null);
  }

  public static EventGatewayBuilder builder() {
    return new EventGatewayBuilder();
  }

  public static class EventGatewayBuilder {

    private Properties producerConfig;
    private ObjectMapper objectMapper;

    public EventGatewayBuilder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      this.producerConfig.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

      return this;
    }

    public EventGatewayBuilder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public DefaultEventGateway build() {
      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      return new DefaultEventGateway(
          this.producerConfig,
          this.objectMapper);
    }
  }
}
