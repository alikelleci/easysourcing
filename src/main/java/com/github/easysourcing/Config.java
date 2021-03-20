package com.github.easysourcing;

import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.Properties;

@Slf4j
@Value
@Builder(toBuilder = true)
public class Config {

  private String bootstrapServers;
  private String applicationId;
  @Builder.Default
  private int replicas = 1;
  @Builder.Default
  private int partitions = 1;
  @Builder.Default
  private String securityProtocol = "PLAINTEXT";
  @Builder.Default
  private boolean rebuildLocalState = false;
  @Builder.Default
  private boolean inMemoryStateStore = false;

  @Builder.Default
  private long commandsRetention = 604800000; // 7 days
  @Builder.Default
  private long resultsRetention = 604800000; // 7 days
  @Builder.Default
  private long snapshotsRetention = 86400000; // 1 day
  @Builder.Default
  private long eventsRetention = -1; // infinite


  public Properties streamsConfig() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, replicas);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

    return properties;
  }

  public Properties producerConfigs() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

    return properties;
  }

  public Properties adminConfigs() {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);

    return properties;
  }
}
