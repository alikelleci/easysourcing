package com.github.easysourcing;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.MessageStream;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.aggregates.annotations.ApplyEvent;
import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.commands.CommandHandler;
import com.github.easysourcing.messages.commands.CommandStream;
import com.github.easysourcing.messages.commands.annotations.HandleCommand;
import com.github.easysourcing.messages.events.EventHandler;
import com.github.easysourcing.messages.events.EventStream;
import com.github.easysourcing.messages.events.annotations.HandleEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.config.TopicBuilder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class EasySourcingBuilder {

  private Config config;

  //  Handlers
  private final ConcurrentMap<Class<?>, CommandHandler> commandHandlers = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<?>, Aggregator> aggregators = new ConcurrentHashMap<>();
  private final ConcurrentMap<Class<?>, EventHandler> eventHandlers = new ConcurrentHashMap<>();


  public EasySourcingBuilder() {
  }

  public EasySourcingBuilder withConfig(Config config) {
    this.config = config;
    return this;
  }

  public EasySourcingBuilder registerHandler(Object handler) {
    List<Method> commandHandlerMethods = MethodUtils.getMethodsListWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> aggregatorMethods = MethodUtils.getMethodsListWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> eventHandlerMethods = MethodUtils.getMethodsListWithAnnotation(handler.getClass(), HandleEvent.class);

    commandHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));

    aggregatorMethods
        .forEach(method -> addAggregator(handler, method));

    eventHandlerMethods
        .forEach(method -> addEventHandler(handler, method));

    return this;
  }

  public EasySourcing build() {
    if (this.config == null) {
      throw new RuntimeException("No config provided!");
    }

    createTopics();
    Topology topology = buildTopology();

    return new EasySourcing(this.config, topology);
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      commandHandlers.put(type, new CommandHandler(listener, method));
    }
  }

  private void addAggregator(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      aggregators.put(type, new Aggregator(listener, method));
    }
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventHandlers.put(type, new EventHandler(listener, method));
    }
  }

  private Set<String> getCommandsTopics() {
    return commandHandlers.keySet().stream()
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getEvensTopics() {
    return Stream.of(eventHandlers.keySet(), aggregators.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getTopics() {
    Set<String> topics = new HashSet<>();
    topics.addAll(getCommandsTopics());
    topics.addAll(getEvensTopics());

    return topics;
  }

  private void createTopics() {
    try (AdminClient adminClient = AdminClient.create(config.adminConfigs())) {
      Set<String> brokerTopics = adminClient.listTopics().listings().get()
          .stream()
          .map(TopicListing::name)
          .collect(Collectors.toSet());

      Set<NewTopic> topicsToCreate = getTopics().stream()
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> TopicBuilder.name(topic)
              .partitions(config.getPartitions())
              .replicas(config.getReplicas())
              .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
              .build())
          .collect(Collectors.toSet());

      CreateTopicsOptions options = new CreateTopicsOptions();
      options.timeoutMs(15000);

      adminClient.createTopics(topicsToCreate, options).all().get();

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  private Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    Set<String> topics = getTopics();
    if (!topics.isEmpty()) {
      MessageStream messageStream = new MessageStream(topics);
      KStream<String, Message> messageKStream = messageStream.buildStream(builder);

      CommandStream commandStream = new CommandStream(commandHandlers, aggregators);
      commandStream.buildStream(messageKStream);

      EventStream eventStream = new EventStream(eventHandlers);
      eventStream.buildStream(messageKStream);
    }

    return builder.build();
  }

}
