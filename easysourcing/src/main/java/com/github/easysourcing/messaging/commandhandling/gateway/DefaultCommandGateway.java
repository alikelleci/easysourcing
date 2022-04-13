package com.github.easysourcing.messaging.commandhandling.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.easysourcing.common.annotations.TopicInfo;
import com.github.easysourcing.common.exceptions.AggregateIdMissingException;
import com.github.easysourcing.common.exceptions.PayloadMissingException;
import com.github.easysourcing.common.exceptions.TopicInfoMissingException;
import com.github.easysourcing.messaging.Message;
import com.github.easysourcing.messaging.Metadata;
import com.github.easysourcing.messaging.commandhandling.Command;
import com.github.easysourcing.messaging.commandhandling.exceptions.CommandExecutionException;
import com.github.easysourcing.support.serializer.JsonSerializer;
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

import static com.github.easysourcing.messaging.Metadata.CORRELATION_ID;
import static com.github.easysourcing.messaging.Metadata.ID;
import static com.github.easysourcing.messaging.Metadata.REPLY_TO;

@Slf4j
public class DefaultCommandGateway extends AbstractCommandResultListener implements CommandGateway {

  //private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final Producer<String, Command> producer;

  protected DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic) {
    super(consumerConfig, replyTopic);

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
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
        .metadata(metadata.filter().toBuilder()
            .entry(ID, UUID.randomUUID().toString())
            .entry(CORRELATION_ID, UUID.randomUUID().toString())
            .entry(REPLY_TO, getReplyTopic())
            .build())
        .build();

    validate(command);
    ProducerRecord<String, Command> record = new ProducerRecord<>(command.getTopicInfo().value(), null, timestamp.toEpochMilli(), command.getAggregateId(), command);

    log.debug("Sending command: {} ({})", command.getType(), command.getAggregateId());
    producer.send(record);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getMetadata().get(ID), future);

    return (CompletableFuture<R>) future;
  }

  @Override
  protected void onMessage(ConsumerRecords<String, Command> consumerRecords) {
    consumerRecords.forEach(record -> {
      String messageId = Optional.ofNullable(record.value().getMetadata())
          .map(metadata -> metadata.get(ID))
          .orElse(null);

      if (StringUtils.isBlank(messageId)) {
        return;
      }
      // CompletableFuture<Object> future = futures.remove(messageId);
      CompletableFuture<Object> future = cache.getIfPresent(messageId);
      if (future != null) {
        Exception exception = checkForErrors(record);
        if (exception == null) {
          future.complete(record.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(messageId);
      }
    });
  }

  private void validate(Command message) {
    if (message.getPayload() == null) {
      throw new PayloadMissingException("You are trying to dispatch a message without a payload.");
    }

    TopicInfo topicInfo = message.getTopicInfo();
    if (topicInfo == null) {
      throw new TopicInfoMissingException("You are trying to dispatch a message without any topic information. Please annotate your message with @TopicInfo.");
    }

    String aggregateId = message.getAggregateId();
    if (aggregateId == null) {
      throw new AggregateIdMissingException("You are trying to dispatch a message without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
    }
  }

  private Exception checkForErrors(ConsumerRecord<String, Command> record) {
    Message message = record.value();
    Metadata metadata = message.getMetadata();

    if (metadata.get(Metadata.RESULT).equals("failed")) {
      return new CommandExecutionException(metadata.get(Metadata.FAILURE));
    }

    return null;
  }

}
