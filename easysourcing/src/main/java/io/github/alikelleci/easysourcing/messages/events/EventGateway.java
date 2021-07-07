package io.github.alikelleci.easysourcing.messages.events;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

@Slf4j
public class EventGateway {

  private final Producer<String, Object> producer;

  public EventGateway(Producer<String, Object> producer) {
    this.producer = producer;
  }

  public Future<RecordMetadata> publish(Object payload, Metadata metadata) {
    CommonUtils.validatePayload(payload);

    ProducerRecord<String, Object> record = new ProducerRecord<>(CommonUtils.getTopicInfo(payload).value(), CommonUtils.getAggregateId(payload), payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }
    metadata.filter().getEntries().forEach((key, value) ->
        record.headers()
            .remove(key)
            .add(key, value.toString().getBytes(StandardCharsets.UTF_8)));

    log.debug("Publishing event: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    return producer.send(record);
  }

  public Future<RecordMetadata> publish(Object payload) {
    return this.publish(payload, null);
  }

}
