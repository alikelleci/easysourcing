package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.commands.DefaultCommandGateway;
import io.github.alikelleci.easysourcing.messages.events.DefaultEventGateway;
import io.github.alikelleci.easysourcing.messages.events.EventGateway;
import io.github.alikelleci.easysourcing.support.serializer.JsonDeserializer;
import io.github.alikelleci.easysourcing.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class GatewayBuilder {

  private final Properties producerConfig;

  public GatewayBuilder(Properties producerConfig) {
    this.producerConfig = producerConfig;
    this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
  }

  public DefaultCommandGateway commandGateway(String replyTopic) {
    return new DefaultCommandGateway(producer(), consumer(), replyTopic);
  }

  public EventGateway eventGateway() {
    return new DefaultEventGateway(producer());
  }

  private Producer<String, Object> producer() {
    return new KafkaProducer<>(this.producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  private Consumer<String, JsonNode> consumer() {
    return new KafkaConsumer<>(consumerConfig(),
        new StringDeserializer(),
        new JsonDeserializer<>(JsonNode.class));
  }

  private Properties consumerConfig() {
    Properties properties = new Properties();
    properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "command-gateway-" + UUID.randomUUID().toString());
    properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    String bootstrapServers = this.producerConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (StringUtils.isNotBlank(bootstrapServers)) {
      properties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    String protocol = this.producerConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (StringUtils.isNotBlank(protocol)) {
      properties.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
    }


    return properties;
  }

}
