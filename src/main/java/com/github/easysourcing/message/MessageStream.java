package com.github.easysourcing.message;


import com.github.easysourcing.kafka.streams.serdes.CustomJsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Slf4j
@Component
public class MessageStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Value("${spring.kafka.streams.replication-factor:1}")
  private int REPLICATION_FACTOR;


  @Bean
  public NewTopic eventsTopic() {
    return TopicBuilder.name(APPLICATION_ID.concat("-events"))
        .partitions(3)
        .replicas(REPLICATION_FACTOR)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Bean
  public KStream<String, Message> messageKStream(StreamsBuilder builder) {
    return builder
        .stream(Pattern.compile("(.*)-events"), Consumed.with(Serdes.String(), new CustomJsonSerde<>(Message.class)))
//        .peek((key, message) -> log.debug("Message received: {}", message))
        .filter((key, message) -> key != null)
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getType() != null)
        .filter((key, message) -> message.getName() != null)
        .filter((key, message) -> message.getPayload() != null)
        .filter((key, message) -> message.getAggregateId() != null);
  }


}
