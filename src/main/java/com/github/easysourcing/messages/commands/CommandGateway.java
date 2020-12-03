package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;

import java.util.UUID;

import static com.github.easysourcing.messages.MetadataKeys.CORRELATION_ID;
import static com.github.easysourcing.messages.MetadataKeys.ID;

@Slf4j
public class CommandGateway extends MessageGateway {

  public CommandGateway(Producer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public void send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Command command = Command.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(ID, UUID.randomUUID().toString())
            .entry(CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    if (log.isDebugEnabled()) {
      log.debug("Sending command: {}", command);
    } else if (log.isInfoEnabled()) {
      log.info("Sending command: {} ({})", command.getType(), command.getAggregateId());
    }
    send(command);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}
