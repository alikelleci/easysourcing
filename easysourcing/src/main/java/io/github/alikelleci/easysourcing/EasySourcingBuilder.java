package io.github.alikelleci.easysourcing;

import io.github.alikelleci.easysourcing.core.events.EventSourcedStream;
import io.github.alikelleci.easysourcing.core.exceptions.annotations.HandleException;
import io.github.alikelleci.easysourcing.util.HandlerUtils;
import io.github.alikelleci.easysourcing.core.aggregates.Aggregator;
import io.github.alikelleci.easysourcing.core.aggregates.annotations.ApplyEvent;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.core.commands.CommandHandler;
import io.github.alikelleci.easysourcing.core.commands.CommandStream;
import io.github.alikelleci.easysourcing.core.commands.annotations.HandleCommand;
import io.github.alikelleci.easysourcing.core.events.EventHandler;
import io.github.alikelleci.easysourcing.core.events.EventStream;
import io.github.alikelleci.easysourcing.core.events.annotations.HandleEvent;
import io.github.alikelleci.easysourcing.core.exceptions.ExceptionHandler;
import io.github.alikelleci.easysourcing.core.exceptions.ExceptionStream;
import io.github.alikelleci.easysourcing.core.snapshots.SnapshotHandler;
import io.github.alikelleci.easysourcing.core.snapshots.SnapshotStream;
import io.github.alikelleci.easysourcing.core.snapshots.annotations.HandleSnapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class EasySourcingBuilder {

  private final Properties streamsConfig;

  private KafkaStreams.StateListener stateListener;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
  private boolean inMemoryStateStore;
  private OperationMode operationMode = OperationMode.NORMAL;

  //  Handlers
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, Aggregator> aggregators = new HashMap<>();
  private final MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers = new ArrayListValuedHashMap<>();


  public EasySourcingBuilder(Properties streamsConfig) {
    this.streamsConfig = streamsConfig;
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
  }

  public EasySourcingBuilder setStateListener(KafkaStreams.StateListener stateListener) {
    this.stateListener = stateListener;
    return this;
  }

  public EasySourcingBuilder setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
    this.uncaughtExceptionHandler = exceptionHandler;
    return this;
  }

  public EasySourcingBuilder setInMemoryStateStore(boolean inMemoryStateStore) {
    this.inMemoryStateStore = inMemoryStateStore;
    return this;
  }

  public EasySourcingBuilder setOperationMode(OperationMode operationMode) {
    this.operationMode = operationMode;
    return this;
  }

  public EasySourcingBuilder registerHandler(Object handler) {
    List<Method> commandHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> aggregatorMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> exceptionHandlerMethods  = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleException.class);
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);
    List<Method> snapshotHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleSnapshot.class);

    commandHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));

    aggregatorMethods
        .forEach(method -> addAggregator(handler, method));

    exceptionHandlerMethods
        .forEach(method -> addExceptionHandler(handler, method));

    eventHandlerMethods
        .forEach(method -> addEventHandler(handler, method));

    snapshotHandlerMethods
        .forEach(method -> addSnapshotHandler(handler, method));

    return this;
  }

  public EasySourcing build() {
    createTopics();
    Topology topology = buildTopology();
    return new EasySourcing(topology, this.streamsConfig, stateListener, uncaughtExceptionHandler);
  }

  private Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    if (operationMode != OperationMode.NORMAL) {
      log.warn("Operation mode is set to {}", operationMode);
      Set<String> eventSourcedTopics = getEventSourcedTopics();
      if (CollectionUtils.isNotEmpty(eventSourcedTopics)) {
        EventSourcedStream eventSourcedStream = new EventSourcedStream(eventSourcedTopics, aggregators, inMemoryStateStore, operationMode);
        eventSourcedStream.buildStream(builder);
      }
      return builder.build();
    }

    Set<String> commandsTopics = getCommandsTopics();
    if (CollectionUtils.isNotEmpty(commandsTopics)) {
      CommandStream commandStream = new CommandStream(commandsTopics, commandHandlers, aggregators, inMemoryStateStore);
      commandStream.buildStream(builder);
    }

    Set<String> exceptionsTopics  = getExceptionsTopics();
    if (CollectionUtils.isNotEmpty(exceptionsTopics )) {
      ExceptionStream exceptionStream = new ExceptionStream(exceptionsTopics , exceptionHandlers);
      exceptionStream.buildStream(builder);
    }

    Set<String> eventsTopics = getEventsTopics();
    if (CollectionUtils.isNotEmpty(eventsTopics)) {
      EventStream eventStream = new EventStream(eventsTopics, eventHandlers);
      eventStream.buildStream(builder);
    }

    Set<String> snapshotTopics = getSnapshotTopics();
    if (CollectionUtils.isNotEmpty(snapshotTopics)) {
      SnapshotStream snapshotStream = new SnapshotStream(snapshotTopics, snapshotHandlers);
      snapshotStream.buildStream(builder);
    }

    return builder.build();
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

  private void addExceptionHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      exceptionHandlers.put(type, new ExceptionHandler(listener, method));
    }
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      eventHandlers.put(type, new EventHandler(listener, method));
    }
  }

  private void addSnapshotHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      snapshotHandlers.put(type, new SnapshotHandler(listener, method));
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

  private Set<String> getExceptionsTopics() {
    return Stream.of(exceptionHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(s -> s.concat(".exceptions"))
        .collect(Collectors.toSet());
  }

  private Set<String> getEventsTopics() {
    return Stream.of(eventHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getSnapshotTopics() {
    return Stream.of(snapshotHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getEventSourcedTopics() {
    return Stream.of(aggregators.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private void createTopics() {
    Properties properties = new Properties();

    String boostrapServers = streamsConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (StringUtils.isNotBlank(boostrapServers)) {
      properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    }

    String securityProtocol = streamsConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
    if (StringUtils.isNotBlank(securityProtocol)) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    }

    try (AdminClient adminClient = AdminClient.create(properties)) {
      ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
      listTopicsOptions.timeoutMs(15000);
      Set<String> brokerTopics = adminClient.listTopics(listTopicsOptions).names().get();

      Set<NewTopic> topicsToCreate = Stream.of(
          getCommandsTopics(),
          getExceptionsTopics(),
          getEventsTopics(),
          getSnapshotTopics(),
          getEventSourcedTopics()
      )
          .flatMap(Collection::stream)
          .filter(topic -> !brokerTopics.contains(topic))
          .map(topic -> new NewTopic(topic, 1, (short) 1))
          .collect(Collectors.toSet());

      CreateTopicsOptions options = new CreateTopicsOptions();
      options.timeoutMs(15000);
      adminClient.createTopics(topicsToCreate, options).all().get();

    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }
}
