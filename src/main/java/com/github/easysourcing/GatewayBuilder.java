package com.github.easysourcing;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.commands.CommandGateway;
import com.github.easysourcing.messages.events.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
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
      throw new IllegalStateException("No config provided!");
    }
    return new CommandGateway(KafkaProducer());
  }

  public EventGateway eventGateway() {
    if (this.config == null) {
      throw new IllegalStateException("No config provided!");
    }
    return new EventGateway(KafkaProducer());
  }

  private KafkaProducer<String, Message> KafkaProducer() {
    return new KafkaProducer<>(config.producerConfigs(),
        new StringSerializer(),
        new JsonSerializer<Message>()
            .noTypeInfo());
  }

}
