package com.github.easysourcing;

import com.github.easysourcing.message.aggregates.AggregateHandler;
import com.github.easysourcing.message.aggregates.annotations.ApplyEvent;
import com.github.easysourcing.message.annotations.TopicInfo;
import com.github.easysourcing.message.commands.CommandHandler;
import com.github.easysourcing.message.commands.CommandStream;
import com.github.easysourcing.message.commands.annotations.HandleCommand;
import com.github.easysourcing.message.events.EventHandler;
import com.github.easysourcing.message.events.EventStream;
import com.github.easysourcing.message.events.annotations.HandleEvent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.config.TopicBuilder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Getter
public class EasySourcing {

  private Config config;

  //  Handlers
  private final ConcurrentMap<Class<?>, CommandHandler> commandHandlers = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<?>, AggregateHandler> aggregateHandlers = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<?>, EventHandler> eventHandlers = new ConcurrentHashMap<>();

  // Topics
  private final ConcurrentMap<String, NewTopic> commandTopics = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, NewTopic> eventTopics = new ConcurrentHashMap<>();

  // Kafka
  private KafkaStreams kafkaStreams;
  private AdminClient adminClient;

  private boolean running = false;

  protected EasySourcing(Config config) {
    this.config = config;
  }

  private Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    if (!commandTopics.isEmpty()) {
      CommandStream commandStream = new CommandStream(commandTopics.keySet(), commandHandlers, aggregateHandlers);
      commandStream.buildStream(builder);
    }

    if (!eventTopics.isEmpty()) {
      EventStream eventStream = new EventStream(eventTopics.keySet(), eventHandlers);
      eventStream.buildStream(builder);
    }

    return builder.build();
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener((newState, oldState) -> log.warn("State changed from {} to {}", oldState, newState));
    kafkaStreams.setUncaughtExceptionHandler((t, e) -> log.error("Exception handler triggered ", e));
  }

  private void addShutdownHook() {
    CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      stop();
      latch.countDown();
    }));
  }

  public void start() {
    if (running) {
      log.warn("Easy Sourcing already started.");
      return;
    }

    createTopics();

    Topology topology = buildTopology();
    kafkaStreams = new KafkaStreams(topology, config.streamsConfig());

    addShutdownHook();
    setUpListeners();

    kafkaStreams.start();
    this.running = true;
  }

  public void stop() {
    if (!running) {
      log.warn("Easy Sourcing already stopped.");
      return;
    }

    if (kafkaStreams != null) {
      log.info("Kafka Streams is shutting down.");
      kafkaStreams.close();
      kafkaStreams = null;
      running = false;
    }
  }

  private void createTopics() {
    this.adminClient = AdminClient.create(config.adminConfigs());
    try {
      Set<String> brokerTopics = adminClient.listTopics().listings().get()
          .stream()
          .map(TopicListing::name)
          .collect(Collectors.toSet());

      Set<NewTopic> topicsToCreate = Stream.of(commandTopics.values(), eventTopics.values())
          .flatMap(Collection::stream)
          .filter(newTopic -> !brokerTopics.contains(newTopic.name()))
          .collect(Collectors.toSet());

      CreateTopicsOptions options = new CreateTopicsOptions();
      options.timeoutMs(15000);

      adminClient.createTopics(topicsToCreate, options).all().get();

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();

    } finally {
      adminClient.close();
    }
  }

  protected void registerHandler(Object handler) {
    List<Method> commandHanderMethods = MethodUtils.getMethodsListWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> aggregateHanderMethods = MethodUtils.getMethodsListWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> eventHanderMethods = MethodUtils.getMethodsListWithAnnotation(handler.getClass(), HandleEvent.class);

    commandHanderMethods
        .forEach(method -> addCommandHandler(handler, method));

    aggregateHanderMethods
        .forEach(method -> addAggregatedHandler(handler, method));

    eventHanderMethods
        .forEach(method -> addEventdHandler(handler, method));
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        commandHandlers.put(type, new CommandHandler(listener, method));
        commandTopics.put(topicInfo.value(), TopicBuilder.name(topicInfo.value())
            .partitions(config.getPartitions())
            .replicas(config.getReplicas())
            .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
            .build());
      }
    }
  }

  private void addAggregatedHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        aggregateHandlers.put(type, new AggregateHandler(listener, method));
        eventTopics.put(topicInfo.value(), TopicBuilder.name(topicInfo.value())
            .partitions(config.getPartitions())
            .replicas(config.getReplicas())
            .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
            .build());
      }
    }
  }

  private void addEventdHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        eventHandlers.put(type, new EventHandler(listener, method));
        eventTopics.put(topicInfo.value(), TopicBuilder.name(topicInfo.value())
            .partitions(config.getPartitions())
            .replicas(config.getReplicas())
            .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
            .build());
      }
    }
  }


}
