package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.support.serializer.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.thavam.util.concurrent.blockingMap.BlockingHashMap;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
public class RequestReplyGateway extends CommandGateway {

  private final String replyTopic;
  private Consumer<String, CommandResult> consumer;
  private BlockingMap<String, CommandResult> results = new BlockingHashMap<>();

  private boolean running = true;


  public RequestReplyGateway(Producer<String, Object> producer, String replyTopic) {
    super(producer);
    this.replyTopic = replyTopic;
    startConsumer(replyTopic);
  }

  private void startConsumer(String replyTopic) {
    consumer = new KafkaConsumer<>(properties(), new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    consumer.subscribe(Collections.singletonList(replyTopic));

    Thread thread = new Thread(() -> {
      while (running) {
        ConsumerRecords<String, CommandResult> consumerRecords = consumer.poll(Duration.ofMillis(100));
        consumerRecords.forEach(record -> {

          String correlationId = Optional.ofNullable(record.headers().lastHeader(Metadata.CORRELATION_ID))
              .map(Header::value)
              .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
              .orElse(null);

          if (StringUtils.isNotBlank(correlationId)) {
            results.put(correlationId, record.value());
          }
        });
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      running = false;
      consumer.close();
    }));

    thread.start();
  }

  protected Future<CommandResult> sendAndWait(ProducerRecord<String, Object> record) {
    record.headers()
        .remove(Metadata.REPLY_TO)
        .add(Metadata.REPLY_TO, replyTopic.getBytes(StandardCharsets.UTF_8));

    this.send(record);

    return CompletableFuture.supplyAsync(() -> {
      try {
        String correlationId = Optional.ofNullable(record.headers().lastHeader(Metadata.CORRELATION_ID))
            .map(Header::value)
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .orElse(null);

        return results.take(correlationId);
      } catch (InterruptedException e) {
        e.printStackTrace();
        return null;
      }
    });
  }

  private Properties properties() {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "command-gateway-" + UUID.randomUUID().toString());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");

    return properties;
  }
}
