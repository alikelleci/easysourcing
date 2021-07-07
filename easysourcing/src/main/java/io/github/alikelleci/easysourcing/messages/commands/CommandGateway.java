package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

@Slf4j
public class CommandGateway {

  private final Producer<String, Object> producer;

  public CommandGateway(Producer<String, Object> producer) {
    this.producer = producer;
  }

  public Future<RecordMetadata> send(Object payload, Metadata metadata) {
    CommonUtils.validatePayload(payload);

    ProducerRecord<String, Object> record = new ProducerRecord<>(CommonUtils.getTopicInfo(payload).value(), CommonUtils.getAggregateId(payload), payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }
    metadata.filter().getEntries().entrySet().stream()
        .filter(entry -> StringUtils.isNoneBlank(entry.getKey(), entry.getValue()))
        .forEach(entry ->
            record.headers()
                .remove(entry.getKey())
                .add(entry.getKey(), entry.getValue().getBytes(StandardCharsets.UTF_8)));

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    return producer.send(record);
  }

  public Future<RecordMetadata> send(Object payload) {
    return this.send(payload, null);
  }

}
