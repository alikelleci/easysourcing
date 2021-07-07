package io.github.alikelleci.easysourcing.support.interceptors;

import io.github.alikelleci.easysourcing.common.annotations.Revision;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.core.annotation.AnnotationUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class CommonProducerInterceptor implements ProducerInterceptor<String, Object> {

  @Override
  public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> producerRecord) {
    int revision = Optional.ofNullable(AnnotationUtils.findAnnotation(producerRecord.value().getClass(), Revision.class))
        .map(Revision::value)
        .orElse(1);

    producerRecord.headers()
        .remove("$id")
        .add("$id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
        .remove("$revision")
        .add("$revision", String.valueOf(revision).getBytes(StandardCharsets.UTF_8));

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
