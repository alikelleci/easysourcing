package io.github.alikelleci.easysourcing.messages.events;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Object> producer;

  public DefaultEventGateway(Producer<String, Object> producer) {
    this.producer = producer;
  }

  @Override
  public void publish(Object payload, Metadata metadata) {
    ProducerRecord<String, Object> record = createProducerRecord(payload, metadata);
    log.debug("Publishing event: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    producer.send(record);
  }
}
