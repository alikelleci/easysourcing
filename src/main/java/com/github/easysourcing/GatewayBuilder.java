package com.github.easysourcing;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.MessageGateway;
import com.github.easysourcing.message.commands.CommandGateway;
import com.github.easysourcing.message.events.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Slf4j
public class GatewayBuilder {

  private Config config;
  private KafkaTemplate<String, Message> kafkaTemplate;

  public GatewayBuilder(Config config) {
    this.config = config;
    this.kafkaTemplate = kafkaTemplate();
  }


  private DefaultKafkaProducerFactory producerFactory() {
    return new DefaultKafkaProducerFactory(config.producerConfigs(),
        new StringSerializer(),
        new JsonSerializer<>()
            .noTypeInfo());
  }

  private KafkaTemplate<String, Message> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  public CommandGateway commandGateway() {
    if (kafkaTemplate == null) {
      kafkaTemplate = kafkaTemplate();
    }
    return new CommandGateway(new MessageGateway(kafkaTemplate));
  }

  public EventGateway eventGateway() {
    if (kafkaTemplate == null) {
      kafkaTemplate = kafkaTemplate();
    }
    return new EventGateway(new MessageGateway(kafkaTemplate));
  }

}
