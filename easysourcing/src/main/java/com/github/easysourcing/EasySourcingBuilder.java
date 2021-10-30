package com.github.easysourcing;

import com.github.easysourcing.constants.Handlers;
import com.github.easysourcing.constants.Topics;
import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.aggregates.annotations.ApplyEvent;
import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.commands.CommandHandler;
import com.github.easysourcing.messages.commands.annotations.HandleCommand;
import com.github.easysourcing.messages.events.EventHandler;
import com.github.easysourcing.messages.events.annotations.HandleEvent;
import com.github.easysourcing.messages.results.ResultHandler;
import com.github.easysourcing.messages.results.annotations.HandleResult;
import com.github.easysourcing.messages.snapshots.SnapshotHandler;
import com.github.easysourcing.messages.snapshots.annotations.HandleSnapshot;
import com.github.easysourcing.utils.HandlerUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
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
    return new EasySourcing(
        this.streamsConfig,
        stateListener,
        uncaughtExceptionHandler);
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      Handlers.COMMAND_HANDLERS.put(type, new CommandHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.COMMANDS.add(topicInfo.value());
      }
    }
  }

  private void addAggregator(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[1].getType();
      Handlers.AGGREGATORS.put(type, new Aggregator(listener, method));

//      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
//      if (topicInfo != null) {
//        Topics.EVENTS.add(topicInfo.value());
//      }
    }
  }

  private void addResultHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.RESULT_HANDLERS.put(type, new ResultHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.RESULTS.add(topicInfo.value().concat(".results"));
      }
    }
  }

  private void addEventHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.EVENT_HANDLERS.put(type, new EventHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.EVENTS.add(topicInfo.value());
      }
    }
  }

  private void addSnapshotHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      Handlers.SNAPSHOT_HANDLERS.put(type, new SnapshotHandler(listener, method));

      TopicInfo topicInfo = AnnotationUtils.findAnnotation(type, TopicInfo.class);
      if (topicInfo != null) {
        Topics.SNAPSHOTS.add(topicInfo.value());
      }
    }
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
              Topics.COMMANDS,
              Topics.EVENTS,
              Topics.RESULTS,
              Topics.SNAPSHOTS
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
