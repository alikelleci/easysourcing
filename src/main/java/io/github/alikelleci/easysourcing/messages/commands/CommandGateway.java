package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.Message;
import io.github.alikelleci.easysourcing.messages.MessageGateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.MetadataKeys;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.UUID;
import java.util.concurrent.Future;

@Slf4j
public class CommandGateway extends MessageGateway {

  public CommandGateway(Producer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public Future<RecordMetadata> send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Command command = Command.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(MetadataKeys.ID, UUID.randomUUID().toString())
            .entry(MetadataKeys.CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    log.debug("Sending command: {} ({})", command.getType(), command.getAggregateId());
    return send(command);
  }

  public Future<RecordMetadata> send(Object payload) {
    return this.send(payload, null);
  }

}
