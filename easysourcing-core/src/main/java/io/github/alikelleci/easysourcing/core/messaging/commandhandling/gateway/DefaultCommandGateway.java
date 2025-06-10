package io.github.alikelleci.easysourcing.core.messaging.commandhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.easysourcing.core.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.core.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.PayloadMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.easysourcing.core.messaging.Message;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.easysourcing.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DefaultCommandGateway extends AbstractCommandResultListener implements CommandGateway {

  //private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final Producer<String, Command> producer;

  protected DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper) {
    super(consumerConfig, replyTopic, objectMapper);

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(Command.class, objectMapper));
  }

  @Override
  public <R> CompletableFuture<R> send(Object payload, Metadata metadata, Instant timestamp) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    if (timestamp == null) {
      timestamp = Instant.now();
    }

    Command command = Command.builder()
        .payload(payload)
        .metadata(Metadata.builder()
            .addAll(metadata)
            .add(Metadata.CORRELATION_ID, UUID.randomUUID().toString())
            .add(Metadata.REPLY_TO, getReplyTopic())
            .build())
        .build();

    validate(command);
    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(command.getTopicInfo().value(), null, timestamp.toEpochMilli(), command.getAggregateId(), command);

    log.debug("Sending command: {} ({})", command.getType(), command.getAggregateId());
    producer.send(producerRecord);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getMetadata().get(Metadata.ID), future);

    return (CompletableFuture<R>) future;
  }

  @Override
  protected void onMessage(ConsumerRecords<String, Command> consumerRecords) {
    consumerRecords.forEach(consumerRecord -> {
      String messageId = Optional.ofNullable(consumerRecord.value().getMetadata())
          .map(metadata -> metadata.get(Metadata.ID))
          .orElse(null);

      if (StringUtils.isBlank(messageId)) {
        return;
      }
      // CompletableFuture<Object> future = futures.remove(messageId);
      CompletableFuture<Object> future = cache.getIfPresent(messageId);
      if (future != null) {
        Exception exception = checkForErrors(consumerRecord);
        if (exception == null) {
          future.complete(consumerRecord.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(messageId);
      }
    });
  }

  private void validate(Command command) {
    if (command.getPayload() == null) {
      throw new PayloadMissingException("You are trying to send a command without a payload.");
    }

    TopicInfo topicInfo = command.getTopicInfo();
    if (topicInfo == null) {
      throw new TopicInfoMissingException("You are trying to send a command without any topic information. Please annotate your command with @TopicInfo.");
    }

    String aggregateId = command.getAggregateId();
    if (aggregateId == null) {
      throw new AggregateIdMissingException("You are trying to send a command without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
    }
  }

  private Exception checkForErrors(ConsumerRecord<String, Command> consumerRecord) {
    Message message = consumerRecord.value();
    Metadata metadata = message.getMetadata();

    if (metadata.get(Metadata.RESULT).equals("failed")) {
      return new CommandExecutionException(metadata.get(Metadata.FAILURE));
    }

    return null;
  }

}
