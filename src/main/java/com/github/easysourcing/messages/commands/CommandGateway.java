package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static com.github.easysourcing.messages.MetadataKeys.CORRELATION_ID;
import static com.github.easysourcing.messages.MetadataKeys.ID;

@Slf4j
public class CommandGateway extends MessageGateway {

  public CommandGateway(KafkaTemplate<String, Message> kafkaTemplate) {
    super(kafkaTemplate);
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

    log.info("Sending command: {}", StringUtils.truncate(command.toString(), 1000));
    send(command);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}
