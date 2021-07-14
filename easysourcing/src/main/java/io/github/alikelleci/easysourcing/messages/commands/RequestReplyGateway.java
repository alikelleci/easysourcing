package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RequestReplyGateway extends CommandGateway {

  private final AtomicBoolean closed = new AtomicBoolean(false);
  private Consumer<String, CommandResult> consumer;

  private final String replyTopic;
  private BlockingMap<String, CommandResult> results = new BlockingHashMap<>();

  public RequestReplyGateway(Producer<String, Object> producer, Consumer<String, CommandResult> consumer, String replyTopic) {
    super(producer);
    this.consumer = consumer;
    this.replyTopic = replyTopic;

    startConsumer(replyTopic);
  }

  private void startConsumer(String replyTopic) {
    Thread thread = new Thread(() -> {
      try {
        consumer.subscribe(Collections.singletonList(replyTopic));
        while (!closed.get()) {
          ConsumerRecords<String, CommandResult> consumerRecords = consumer.poll(Duration.ofMillis(100));
          consumerRecords.forEach(record -> {
            String correlationId = getCorrelationId(record.headers());
            if (StringUtils.isNotBlank(correlationId)) {
              results.put(correlationId, record.value());
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


  @Override
  protected Future<RecordMetadata> send(ProducerRecord<String, Object> record) {
    record.headers()
        .remove(Metadata.REPLY_TO)
        .add(Metadata.REPLY_TO, replyTopic.getBytes(StandardCharsets.UTF_8));

    return super.send(record);
  }

  protected Future<CommandResult> sendAndWait(ProducerRecord<String, Object> record) {
    this.send(record);

    return CompletableFuture.supplyAsync(() -> {
      try {
        String correlationId = getCorrelationId(record.headers());
        return results.take(correlationId);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return null;
      }
    });
  }

  public Future<CommandResult> sendAndWait(Object payload, Metadata metadata) {
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

    return this.sendAndWait(record);
  }

  public Future<CommandResult> sendAndWait(Object payload) {
    return this.sendAndWait(payload, null);
  }

}
