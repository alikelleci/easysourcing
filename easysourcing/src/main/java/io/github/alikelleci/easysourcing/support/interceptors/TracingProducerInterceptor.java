package io.github.alikelleci.easysourcing.support.interceptors;

import io.github.alikelleci.easysourcing.messages.Metadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

public class TracingProducerInterceptor implements ProducerInterceptor<String, Object> {

  @Override
  public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> producerRecord) {
    producerRecord.headers()
        .remove(Metadata.CORRELATION_ID)
        .add(Metadata.CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

    return producerRecord;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
