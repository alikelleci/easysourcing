package io.github.easysourcing;

import io.github.easysourcing.messages.HandlerUtils;
import io.github.easysourcing.messages.aggregates.Aggregator;
import io.github.easysourcing.messages.aggregates.annotations.ApplyEvent;
import io.github.easysourcing.messages.annotations.TopicInfo;
import io.github.easysourcing.messages.commands.CommandHandler;
import io.github.easysourcing.messages.commands.CommandStream;
import io.github.easysourcing.messages.commands.annotations.HandleCommand;
import io.github.easysourcing.messages.events.EventHandler;
import io.github.easysourcing.messages.events.EventStream;
import io.github.easysourcing.messages.events.annotations.HandleEvent;
import io.github.easysourcing.messages.results.ResultHandler;
import io.github.easysourcing.messages.results.ResultStream;
import io.github.easysourcing.messages.results.annotations.HandleResult;
import io.github.easysourcing.messages.snapshots.SnapshotHandler;
import io.github.easysourcing.messages.snapshots.SnapshotStream;
import io.github.easysourcing.messages.snapshots.annotations.HandleSnapshot;
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

  //  Handlers
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, Aggregator> aggregators = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers = new ArrayListValuedHashMap<>();


  public EasySourcingBuilder(Properties streamsConfig) {
    this.streamsConfig = streamsConfig;
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
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

  public EasySourcingBuilder registerHandler(Object handler) {
    List<Method> commandHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    List<Method> aggregatorMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), ApplyEvent.class);
    List<Method> resultHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleResult.class);
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);
    List<Method> snapshotHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleSnapshot.class);

    commandHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));

    aggregatorMethods
        .forEach(method -> addAggregator(handler, method));

    resultHandlerMethods
        .forEach(method -> addResultHandler(handler, method));

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

    Set<String> commandsTopics = getCommandsTopics();
    if (CollectionUtils.isNotEmpty(commandsTopics)) {
      CommandStream commandStream = new CommandStream(commandsTopics, commandHandlers, aggregators, inMemoryStateStore);
      commandStream.buildStream(builder);
    }

    Set<String> resultTopics = getResultTopics();
    if (CollectionUtils.isNotEmpty(resultTopics)) {
      ResultStream resultStream = new ResultStream(resultTopics, resultHandlers);
      resultStream.buildStream(builder);
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

  private void addResultHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      resultHandlers.put(type, new ResultHandler(listener, method));
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

  private Set<String> getResultTopics() {
    return Stream.of(resultHandlers.keySet())
        .flatMap(Collection::stream)
        .map(type -> AnnotationUtils.findAnnotation(type, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(s -> s.concat(".results"))
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
          getResultTopics(),
          getEventsTopics(),
          getSnapshotTopics()
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
