package com.github.easysourcing.message;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MessageStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;


  @Autowired
  private AdminClient adminClient;


  @PostConstruct
  private void initTopics() throws ExecutionException, InterruptedException {
    List<String> existingTopics = adminClient.listTopics().listings().get()
        .stream()
        .map(TopicListing::name)
        .collect(Collectors.toList());

    List<String> topics = Arrays.asList(APPLICATION_ID.concat("-events"));
    List<NewTopic> newTopics = new ArrayList<>();
    topics.forEach(topic -> newTopics.add(TopicBuilder.name(topic)
        .partitions(6)
        .replicas(1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build()));

    // filter existing topics and create new topics
    newTopics.removeIf(newTopic -> existingTopics.contains(newTopic.name()));
    adminClient.createTopics(newTopics).all().get();
  }

  @Bean
  public KStream<String, Message> messageKStream(StreamsBuilder builder) {
    // 1.  Read stream
    KStream<String, Message> stream = builder
        .stream(Pattern.compile("(.*)-events"), Consumed.with(Serdes.String(), new JsonSerde<>(Message.class)))
        .filter((key, message) -> key != null)
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getAggregateId() != null)
        .filter((key, message) -> message.getPayload() != null);

    return stream;
  }


}
