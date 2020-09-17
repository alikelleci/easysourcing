package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.Metadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.UUID;

import static com.github.easysourcing.messages.MetadataKeys.CORRELATION_ID;
import static com.github.easysourcing.messages.MetadataKeys.ID;

@Slf4j
public class EventGateway extends MessageGateway {

  public EventGateway(KafkaProducer<String, Message> kafkaProducer) {
    super(kafkaProducer);
  }

  public void send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Event event = Event.builder()
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(ID, UUID.randomUUID().toString())
            .entry(CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    log.info("Sending event: {}", StringUtils.truncate(event.toString(), 1000));
    send(event);
  }

  public void send(Object payload) {
    this.send(payload, null);
  }

}
