package com.github.easysourcing;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.commands.CommandGateway;
import com.github.easysourcing.messages.events.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Slf4j
public class GatewayBuilder {

  private Config config;

  public GatewayBuilder() {
  }

  public GatewayBuilder withConfig(Config config) {
    this.config = config;
    return this;
  }

  public CommandGateway commandGateway() {
    if (this.config == null) {
      throw new RuntimeException("No config provided!");
    }
    return new CommandGateway(kafkaTemplate());
  }

  public EventGateway eventGateway() {
    if (this.config == null) {
      throw new RuntimeException("No config provided!");
    }
    return new EventGateway(kafkaTemplate());
  }

  private DefaultKafkaProducerFactory<String, Message> producerFactory() {
    return new DefaultKafkaProducerFactory<>(config.producerConfigs(),
        new StringSerializer(),
        new JsonSerializer<Message>()
            .noTypeInfo());
  }

  private KafkaTemplate<String, Message> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

}
