package com.github.alikelleci.easysourcing;

import com.github.alikelleci.easysourcing.messages.Message;
import com.github.alikelleci.easysourcing.messages.MessageGateway;
import com.github.alikelleci.easysourcing.messages.commands.CommandGateway;
import com.github.alikelleci.easysourcing.messages.events.EventGateway;
import com.github.alikelleci.easysourcing.messages.snapshots.SnapshotGateway;
import com.github.alikelleci.easysourcing.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class GatewayBuilder {

  private final Properties producerConfig;

  public GatewayBuilder(Properties producerConfig) {
    this.producerConfig = producerConfig;
    this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
  }

  public MessageGateway messageGateway() {
    return new MessageGateway(producer());
  }

  public CommandGateway commandGateway() {
    return new CommandGateway(producer());
  }

  public EventGateway eventGateway() {
    return new EventGateway(producer());
  }

  public SnapshotGateway snapshotGateway() {
    return new SnapshotGateway(producer());
  }

  private Producer<String, Message> producer() {
    return new KafkaProducer<>(this.producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

}
