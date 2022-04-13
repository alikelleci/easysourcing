package io.github.alikelleci.easysourcing.messaging.commandhandling.gateway;

import io.github.alikelleci.easysourcing.messaging.Metadata;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface CommandGateway {

  <R> CompletableFuture<R> send(Object payload, Metadata metadata, Instant timestamp);

  default <R> CompletableFuture<R> send(Object payload, Metadata metadata) {
    return send(payload, metadata, null);
  }

  default <R> CompletableFuture<R> send(Object payload) {
    return send(payload, null, null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, Metadata metadata, Instant timestamp) {
    CompletableFuture<R> future = send(payload, metadata, timestamp);
    return future.get(1, TimeUnit.MINUTES);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload, Metadata metadata) {
    return sendAndWait(payload, metadata, null);
  }

  @SneakyThrows
  default <R> R sendAndWait(Object payload) {
    return sendAndWait(payload, null, null);
  }

  public static CommandGatewayBuilder builder() {
    return new CommandGatewayBuilder();
  }

  public static class CommandGatewayBuilder {

    private Properties producerConfig;
    private Properties consumerConfig;
    private String replyTopic;

    public CommandGatewayBuilder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

      return this;
    }

//    public CommandGatewayBuilder consumerConfig(Properties consumerConfig) {
//      this.consumerConfig = consumerConfig;
//      return this;
//    }

    public CommandGatewayBuilder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public DefaultCommandGateway build() {
      this.consumerConfig = new Properties();

      String bootstrapServers = this.producerConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      if (StringUtils.isNotBlank(bootstrapServers)) {
        this.consumerConfig.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      }

      String securityProtocol = this.producerConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
      if (StringUtils.isNotBlank(securityProtocol)) {
        this.consumerConfig.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      }

      return new DefaultCommandGateway(this.producerConfig, this.consumerConfig, this.replyTopic);
    }
  }

}
