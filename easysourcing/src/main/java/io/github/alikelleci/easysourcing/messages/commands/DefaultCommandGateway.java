package io.github.alikelleci.easysourcing.messages.commands;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.exceptions.CommandExecutionException;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DefaultCommandGateway implements CommandGateway {

  private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Producer<String, Object> producer;
  private final Consumer<String, JsonNode> consumer;
  private final String replyTopic;

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
    ProducerRecord<String, Object> record = createProducerRecord(payload, metadata);
    record.headers()
        .remove(Metadata.REPLY_TO)
        .add(Metadata.REPLY_TO, replyTopic.getBytes(StandardCharsets.UTF_8));

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    producer.send(record);

    String correlationId = getCorrelationId(record.headers());
    CompletableFuture<Object> future = new CompletableFuture<>();
    futures.put(correlationId, future);

    return future;
  }

  public Exception checkForErrors(ConsumerRecord<String, JsonNode> record) {
    String result = new String(record.headers().lastHeader(Metadata.RESULT).value(), StandardCharsets.UTF_8);
    if (result.equals("failure")) {
      String cause = new String(record.headers().lastHeader(Metadata.CAUSE).value(), StandardCharsets.UTF_8);
      return new CommandExecutionException(cause);
    }
    return null;
  }

  private void startConsumer(String replyTopic) {
    Thread thread = new Thread(() -> {
      try {
        consumer.subscribe(Collections.singletonList(replyTopic));
        while (!closed.get()) {
          ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.ofMillis(1000));
          consumerRecords.forEach(record -> {
            String correlationId = getCorrelationId(record.headers());
            CompletableFuture<Object> future = futures.remove(correlationId);
            if (future != null) {
              Exception exception = checkForErrors(record);
              if (exception == null) {
                future.complete(JsonUtils.toJavaType(record.value()));
              } else {
                future.completeExceptionally(exception);
              }
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
