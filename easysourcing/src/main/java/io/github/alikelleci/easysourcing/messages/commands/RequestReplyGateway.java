package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.support.serializer.JsonDeserializer;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.thavam.util.concurrent.blockingMap.BlockingHashMap;
import org.thavam.util.concurrent.blockingMap.BlockingMap;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Slf4j
public class RequestReplyGateway extends CommandGateway {

  private Map<String, CommandResult> results = new ConcurrentHashMap<>();
  private BlockingMap<String, CommandResult> map = new BlockingHashMap<>();
  private Consumer<String, CommandResult> consumer;

  public RequestReplyGateway(Producer<String, Object> producer) {
    super(producer);

    consumer = new KafkaConsumer<>(properties(), new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    consumer.subscribe(Collections.singletonList("my-reply-topic"));

    Thread thread = new Thread(() -> {
      while (true) {
        ConsumerRecords<String, CommandResult> consumerRecords = consumer.poll(Duration.ofMillis(100));
        consumerRecords.forEach(record -> {
          String id = new String(record.headers().lastHeader("x-correlation-id").value(), StandardCharsets.UTF_8);

          results.put(id, record.value());
          map.put(id, record.value());
        });
      }
    });

    thread.start();
  }


  public Future<CommandResult> senddddd(Object payload) {
    CommonUtils.validatePayload(payload);

    String replyTopic = "my-reply-topic";
    String id = UUID.randomUUID().toString();

    ProducerRecord<String, Object> record = new ProducerRecord<>(CommonUtils.getTopicInfo(payload).value(), CommonUtils.getAggregateId(payload), payload);
    record.headers()
        .add("x-reply-topic", replyTopic.getBytes(StandardCharsets.UTF_8))
        .add("x-correlation-id", id.getBytes(StandardCharsets.UTF_8));

    sendd(record);

    return CompletableFuture.supplyAsync(() -> {
      try {
        System.out.println("sssssssssssssssssssssssss");
        return map.take(id);
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.out.println("bbbbbbbbbbbbbbbbbbbbbbbb");
        return null;
      }
    });


  }

  private static Properties properties() {
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
