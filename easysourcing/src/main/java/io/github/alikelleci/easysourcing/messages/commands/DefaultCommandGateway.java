package io.github.alikelleci.easysourcing.messages.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.RecordReceiver;
import io.github.alikelleci.easysourcing.messages.commands.exceptions.CommandExecutionException;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.thavam.util.concurrent.blockingMap.BlockingHashMap;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DefaultCommandGateway implements CommandGateway, RecordReceiver<Object> {

  private final Producer<String, Object> producer;
  private final Consumer<String, JsonNode> consumer;
  private final String replyTopic;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final BlockingMap<String, ConsumerRecord<String, JsonNode>> results = new BlockingHashMap<>();

  public DefaultCommandGateway(Producer<String, Object> producer, Consumer<String, JsonNode> consumer, String replyTopic) {
    this.producer = producer;
    this.consumer = consumer;
    this.replyTopic = replyTopic;
    startConsumer(replyTopic);
  }

  @Override
  public void sendAndForget(Object payload, Metadata metadata) {
    ProducerRecord<String, Object> record = createProducerRecord(payload, metadata);
    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    producer.send(record);
  }

  @Override
  public CompletableFuture<Object> send(Object payload, Metadata metadata) {
    ProducerRecord<String, Object> producerRecord = createProducerRecord(payload, metadata);
    producerRecord.headers()
        .remove(Metadata.REPLY_TO)
        .add(Metadata.REPLY_TO, replyTopic.getBytes(StandardCharsets.UTF_8));

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    producer.send(producerRecord);

    String correlationId = getCorrelationId(producerRecord.headers());
    return CompletableFuture.supplyAsync(() -> receive(correlationId));
  }

  @Override
  @SneakyThrows
  public Object receive(String correlationId) {
    ConsumerRecord<String, JsonNode> consumerRecord = results.take(correlationId, 1, TimeUnit.MINUTES);

    String result = new String(consumerRecord.headers().lastHeader(Metadata.RESULT).value(), StandardCharsets.UTF_8);
    if (result.equals("success")) {
      return JsonUtils.toJavaType(consumerRecord.value());
    } else {
      String cause = new String(consumerRecord.headers().lastHeader(Metadata.CAUSE).value(), StandardCharsets.UTF_8);
      throw new CommandExecutionException(cause);
    }
  }

  private void startConsumer(String replyTopic) {
    Thread thread = new Thread(() -> {
      try {
        consumer.subscribe(Collections.singletonList(replyTopic));
        while (!closed.get()) {
          ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.ofMillis(1000));
          consumerRecords.forEach(record -> {
            String correlationId = getCorrelationId(record.headers());
            if (StringUtils.isNotBlank(correlationId)) {
              results.put(correlationId, record);
            }
          });
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
      } finally {
        consumer.close();
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      closed.set(true);
      consumer.wakeup();
    }));
    thread.start();
  }

  private String getCorrelationId(Headers headers) {
    return Optional.ofNullable(headers.lastHeader(Metadata.CORRELATION_ID))
        .map(Header::value)
        .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
        .orElse(null);
  }
}
