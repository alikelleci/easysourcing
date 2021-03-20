package com.github.easysourcing;

import com.github.easysourcing.messages.HandlerUtils;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.aggregates.annotations.ApplyEvent;
import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.commands.CommandHandler;
import com.github.easysourcing.messages.commands.CommandStream;
import com.github.easysourcing.messages.commands.annotations.HandleCommand;
import com.github.easysourcing.messages.events.EventHandler;
import com.github.easysourcing.messages.events.EventSourcingStream;
import com.github.easysourcing.messages.events.EventStream;
import com.github.easysourcing.messages.events.annotations.HandleEvent;
import com.github.easysourcing.messages.results.ResultHandler;
import com.github.easysourcing.messages.results.ResultStream;
import com.github.easysourcing.messages.results.annotations.HandleResult;
import com.github.easysourcing.messages.snapshots.SnapshotHandler;
import com.github.easysourcing.messages.snapshots.SnapshotStream;
import com.github.easysourcing.messages.snapshots.annotations.HandleSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class EasySourcingBuilder {

  private Config config;
  private EasySourcing.StateListener stateListener;
  private EasySourcing.UncaughtExceptionHandler uncaughtExceptionHandler;

  //  Handlers
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, Aggregator> aggregators = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();

  public EasySourcingBuilder() {
  }

  public EasySourcingBuilder withConfig(Config config) {
    this.config = config;
    return this;
  }

  public EasySourcingBuilder registerHandler(Object handler) {
    List<Method> commandHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> aggregatorMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> resultHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleResult.class);
    List<Method> snapshotHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleSnapshot.class);
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);

    commandHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));

    aggregatorMethods
        .forEach(method -> addAggregator(handler, method));

    resultHandlerMethods
        .forEach(method -> addResultHandler(handler, method));

    snapshotHandlerMethods
        .forEach(method -> addSnapshotHandler(handler, method));

    eventHandlerMethods
        .forEach(method -> addEventHandler(handler, method));

    return this;
  }

  public EasySourcingBuilder withStateListener(EasySourcing.StateListener stateListener) {
    this.stateListener = stateListener;
    return this;
  }

  public EasySourcingBuilder withUncaughtExceptionHandler(EasySourcing.UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    return this;
  }

  public EasySourcing build() {
    if (this.config == null) {
      throw new RuntimeException("No config provided!");
    }

    if (StringUtils.isBlank(config.getBootstrapServers())) {
      throw new RuntimeException("No bootstrap servers provided!");
    }

    if (StringUtils.isBlank(config.getApplicationId())) {
      throw new RuntimeException("No application id provided!");
    }

    createTopics();
    Topology topology = buildTopology();

    return new EasySourcing(this.config, topology, stateListener, uncaughtExceptionHandler);
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

  private void addResultHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      resultHandlers.put(type, new ResultHandler(listener, method));
    }
  }

  private void addSnapshotHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      snapshotHandlers.put(type, new SnapshotHandler(listener, method));
    }
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventHandlers.put(type, new EventHandler(listener, method));
    }
  }

  private Set<String> getCommandsTopics() {
    return Stream.of(commandHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getResultTopics() {
    Set<String> list1 = Stream.of(resultHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(s -> s.concat(".results"))
        .collect(Collectors.toSet());

    Set<String> list2 = Stream.of(commandHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(s -> s.concat(".results"))
        .collect(Collectors.toSet());

    Set<String> topics = new HashSet<>();
    topics.addAll(list1);
    topics.addAll(list2);

    return topics;
  }

  private Set<String> getSnapshotTopics() {
    Set<String> list1 = Stream.of(snapshotHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());

    Set<String> list2 = Stream.of(aggregators.values())
        .flatMap(Collection::stream)
        .map(aggregator -> aggregator.getMethod().getReturnType())
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());

    Set<String> topics = new HashSet<>();
    topics.addAll(list1);
    topics.addAll(list2);

    return topics;
  }

  private Set<String> getEventsTopics() {
    Set<String> list1 = Stream.of(eventHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());

    Set<String> list2 = Stream.of(commandHandlers.values())
        .flatMap(Collection::stream)
        .map(commandHandler -> commandHandler.getMethod().getReturnType())
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());

    Set<String> list3 = Stream.of(aggregators.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());

    Set<String> topics = new HashSet<>();
    topics.addAll(list1);
    topics.addAll(list2);
    topics.addAll(list3);

    return topics;
  }

  private void createTopics() {
    try (AdminClient adminClient = AdminClient.create(config.adminConfigs())) {
      ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
      listTopicsOptions.timeoutMs(15000);
      Set<String> brokerTopics = adminClient.listTopics(listTopicsOptions).names().get();

      // Commands topic
      Set<NewTopic> commandTopicsToCreate = getCommandsTopics().stream()
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> new NewTopic(topic, config.getPartitions(), (short) config.getReplicas())
              .configs(MapUtils.putAll(new HashMap<>(), new String[]{
                  TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.getCommandsRetention())
              })))
          .collect(Collectors.toSet());

      // Results topic
      Set<NewTopic> resultTopicsToCreate = getResultTopics().stream()
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> new NewTopic(topic, config.getPartitions(), (short) config.getReplicas())
              .configs(MapUtils.putAll(new HashMap<>(), new String[]{
                  TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.getResultsRetention())
              })))
          .collect(Collectors.toSet());

      // Snapshots topic
      Set<NewTopic> snapshotTopicsToCreate = getSnapshotTopics().stream()
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> new NewTopic(topic, config.getPartitions(), (short) config.getReplicas())
              .configs(MapUtils.putAll(new HashMap<>(), new String[]{
                  TopicConfig.CLEANUP_POLICY_CONFIG, "compact",
                  TopicConfig.DELETE_RETENTION_MS_CONFIG, String.valueOf(config.getSnapshotsRetention())
              })))
          .collect(Collectors.toSet());

      // Events topic
      Set<NewTopic> eventTopicsToCreate = getEventsTopics().stream()
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> new NewTopic(topic, config.getPartitions(), (short) config.getReplicas())
              .configs(MapUtils.putAll(new HashMap<>(), new String[]{
                  TopicConfig.RETENTION_MS_CONFIG, String.valueOf(config.getEventsRetention())
              })))
          .collect(Collectors.toSet());

      Set<NewTopic> topicsToCreate = new HashSet<>();
      topicsToCreate.addAll(commandTopicsToCreate);
      topicsToCreate.addAll(resultTopicsToCreate);
      topicsToCreate.addAll(snapshotTopicsToCreate);
      topicsToCreate.addAll(eventTopicsToCreate);

      CreateTopicsOptions options = new CreateTopicsOptions();
      options.timeoutMs(15000);
      adminClient.createTopics(topicsToCreate, options).all().get();

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  private Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    if (config.isRebuildLocalState()) {
      Set<String> eventsTopics = getEventsTopics();
      if (!eventsTopics.isEmpty() && !aggregators.isEmpty()) {
        EventSourcingStream eventSourcingStream = new EventSourcingStream(eventsTopics, aggregators);
        eventSourcingStream.buildStream(builder);
      }
      return builder.build();
    }

    Set<String> commandsTopics = getCommandsTopics();
    if (!commandsTopics.isEmpty() && !commandHandlers.isEmpty()) {
      CommandStream commandStream = new CommandStream(commandsTopics, commandHandlers, aggregators);
      commandStream.buildStream(builder, config.isInMemoryStateStore());
    }

    Set<String> resultTopics = getResultTopics();
    if (!resultTopics.isEmpty() && !resultHandlers.isEmpty()) {
      ResultStream resultStream = new ResultStream(resultTopics, resultHandlers);
      resultStream.buildStream(builder);
    }

    Set<String> snapshotTopics = getSnapshotTopics();
    if (!snapshotTopics.isEmpty() && !snapshotHandlers.isEmpty()) {
      SnapshotStream snapshotStream = new SnapshotStream(snapshotTopics, snapshotHandlers);
      snapshotStream.buildStream(builder);
    }

    Set<String> eventsTopics = getEventsTopics();
    if (!eventsTopics.isEmpty() && !eventHandlers.isEmpty()) {
      EventStream eventStream = new EventStream(eventsTopics, eventHandlers);
      eventStream.buildStream(builder);
    }

    return builder.build();
  }

}
