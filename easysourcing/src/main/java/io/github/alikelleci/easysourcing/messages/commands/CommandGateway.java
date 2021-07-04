package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.common.annotations.Revision;
import io.github.alikelleci.easysourcing.messages.Message;
import io.github.alikelleci.easysourcing.messages.MessageGateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;

import static io.github.alikelleci.easysourcing.messages.MetadataKeys.CORRELATION_ID;
import static io.github.alikelleci.easysourcing.messages.MetadataKeys.ID;
import static io.github.alikelleci.easysourcing.messages.MetadataKeys.REVISION;

@Slf4j
public class CommandGateway extends MessageGateway {

  public CommandGateway(Producer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public Future<RecordMetadata> send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = new Metadata();
    }
    Command command = Command.builder()
        .payload(payload)
        .metadata(metadata.filter()
            .add(ID, UUID.randomUUID().toString())
            .add(REVISION, Optional.ofNullable(AnnotationUtils.findAnnotation(payload.getClass(), Revision.class))
                .map(Revision::value)
                .orElse(0))
            .add(CORRELATION_ID, UUID.randomUUID().toString()))
        .build();

    return send(command);
  }

  public Future<RecordMetadata> send(Object payload) {
    return this.send(payload, null);
  }

}
