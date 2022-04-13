package com.github.easysourcing;

import com.github.easysourcing.common.annotations.TopicInfo;
import com.github.easysourcing.messaging.commandhandling.Command;
import com.github.easysourcing.messaging.commandhandling.CommandHandler;
import com.github.easysourcing.messaging.commandhandling.CommandResult;
import com.github.easysourcing.messaging.commandhandling.CommandTransformer;
import com.github.easysourcing.messaging.eventhandling.Event;
import com.github.easysourcing.messaging.eventhandling.EventHandler;
import com.github.easysourcing.messaging.eventhandling.EventTransformer;
import com.github.easysourcing.messaging.eventsourcing.Aggregate;
import com.github.easysourcing.messaging.eventsourcing.EventSourcingHandler;
import com.github.easysourcing.messaging.eventsourcing.EventSourcingTransformer;
import com.github.easysourcing.messaging.resulthandling.ResultHandler;
import com.github.easysourcing.messaging.resulthandling.ResultTransformer;
import com.github.easysourcing.messaging.snapshothandling.SnapshotHandler;
import com.github.easysourcing.messaging.snapshothandling.SnapshotTransformer;
import com.github.easysourcing.support.serializer.CustomSerdes;
import com.github.easysourcing.utils.HandlerUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.Stores;
import org.springframework.core.annotation.AnnotationUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Getter
public class EasySourcing {
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers = new ArrayListValuedHashMap<>();

  private final Properties streamsConfig;
  private StateListener stateListener;
  private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;

  private KafkaStreams kafkaStreams;


  protected EasySourcing(Properties streamsConfig,
                         StateListener stateListener,
                         StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  public static EasySourcingBuilder builder() {
    return new EasySourcingBuilder();
  }

  public Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    /*
     * -------------------------------------------------------------
     * COMMAND HANDLING
     * -------------------------------------------------------------
     */

    if (!getCommandTopics().isEmpty()) {
      // Snapshot store
      builder.addStateStore(Stores
          .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshot-store"), Serdes.String(), CustomSerdes.Json(Aggregate.class))
          .withLoggingEnabled(Collections.emptyMap()));

      // --> Commands
      KStream<String, Command> commands = builder.stream(getCommandTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null)
          .filter((key, command) -> command.getTopicInfo() != null)
          .filter((key, command) -> command.getAggregateId() != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .transformValues(() -> new CommandTransformer(this), "snapshot-store")
          .filter((key, result) -> result != null)
          .filter((key, result) -> result.getCommand() != null);

      // Results --> Events
      KStream<String, Event> events = commandResults
          .filter((key, result) -> result instanceof CommandResult.Success)
          .mapValues((key, result) -> (CommandResult.Success) result)
          .flatMapValues(CommandResult.Success::getEvents)
          .filter((key, event) -> event != null);

      // Events --> Snapshots
      KStream<String, Aggregate> snapshots = events
          .transformValues(() -> new EventSourcingTransformer(this), "snapshot-store")
          .filter((key, aggregate) -> aggregate != null);

      // Results --> Push
      commandResults
          .mapValues(CommandResult::getCommand)
          .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".results"),
              Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

      // Events --> Push
      events
          .to((key, event, recordContext) -> event.getTopicInfo().value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

      // Snapshots --> Push
      snapshots
          .to((key, aggregate, recordContext) -> aggregate.getTopicInfo().value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)));
    }

    /*
     * -------------------------------------------------------------
     * EVENT HANDLING
     * -------------------------------------------------------------
     */

    if (!getEventTopics().isEmpty()) {
      // --> Events
      KStream<String, Event> events = builder.stream(getEventTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
          .filter((key, event) -> key != null)
          .filter((key, event) -> event != null)
          .filter((key, event) -> event.getPayload() != null)
          .filter((key, event) -> event.getTopicInfo() != null)
          .filter((key, event) -> event.getAggregateId() != null);

      // Events --> Void
      events
          .transformValues(() -> new EventTransformer(this));
    }

    /*
     * -------------------------------------------------------------
     * RESULT HANDLING
     * -------------------------------------------------------------
     */

    if (!getResultTopics().isEmpty()) {
      // --> Results
      KStream<String, Command> results = builder.stream(getResultTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null)
          .filter((key, command) -> command.getTopicInfo() != null)
          .filter((key, command) -> command.getAggregateId() != null);

      // Results --> Void
      results
          .transformValues(() -> new ResultTransformer(this));
    }

    /*
     * -------------------------------------------------------------
     * SNAPSHOT HANDLING
     * -------------------------------------------------------------
     */

    if (!getSnapshotTopics().isEmpty()) {
      // --> Snapshots
      KStream<String, Aggregate> snapshots = builder.stream(getSnapshotTopics(), Consumed.with(Serdes.String(), CustomSerdes.Json(Aggregate.class)))
          .filter((key, aggregate) -> key != null)
          .filter((key, aggregate) -> aggregate != null)
          .filter((key, aggregate) -> aggregate.getPayload() != null)
          .filter((key, aggregate) -> aggregate.getTopicInfo() != null)
          .filter((key, aggregate) -> aggregate.getAggregateId() != null);

      // Snapshots --> Void
      snapshots
          .transformValues(() -> new SnapshotTransformer(this));
    }


    return builder.build();
  }

  public void start() {
    if (kafkaStreams != null) {
      log.info("EasySourcing already started.");
      return;
    }

    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("EasySourcing is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    this.kafkaStreams = new KafkaStreams(topology, this.streamsConfig);
    setUpListeners();

    log.info("EasySourcing is starting...");
    kafkaStreams.start();
  }

  public void stop() {
    if (kafkaStreams == null) {
      log.info("EasySourcing already stopped.");
      return;
    }

    log.info("EasySourcing is shutting down...");
    kafkaStreams.close(Duration.ofMillis(5000));
    kafkaStreams = null;
  }

  private void setUpListeners() {
    if (this.stateListener == null) {
      this.stateListener = (newState, oldState) ->
          log.warn("State changed from {} to {}", oldState, newState);
    }
    kafkaStreams.setStateListener(this.stateListener);

    if (this.uncaughtExceptionHandler == null) {
      this.uncaughtExceptionHandler = (throwable) ->
          StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
    }
    kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        log.debug("State restoration started: topic={}, partition={}, store={}, endingOffset={}", topicPartition.topic(), topicPartition.partition(), storeName, endingOffset);
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        log.debug("State restoration in progress: topic={}, partition={}, store={}, numRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, numRestored);
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.debug("State restoration ended: topic={}, partition={}, store={}, totalRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("EasySourcing is shutting down...");
      kafkaStreams.close(Duration.ofMillis(5000));
    }));
  }

  private Set<String> getCommandTopics() {
    return commandHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getEventTopics() {
    return Stream.of(
        eventHandlers.keySet()
//        eventSourcingHandlers.keySet()
    )
        .flatMap(Collection::stream)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  private Set<String> getResultTopics() {
    return resultHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(topic -> topic.concat(".results"))
        .collect(Collectors.toSet());
  }

  private Set<String> getSnapshotTopics() {
    return snapshotHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  public static class EasySourcingBuilder {
    private List<Object> handlers = new ArrayList<>();

    private Properties streamsConfig;
    private StateListener stateListener;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;

    public EasySourcingBuilder registerHandler(Object handler) {
      handlers.add(handler);

      return this;
    }

    public EasySourcingBuilder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

      return this;
    }

    public EasySourcingBuilder stateListener(StateListener stateListener) {
      this.stateListener = stateListener;
      return this;
    }

    public EasySourcingBuilder uncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
      this.uncaughtExceptionHandler = uncaughtExceptionHandler;
      return this;
    }

    public EasySourcing build() {
      EasySourcing easySourcing = new EasySourcing(
          this.streamsConfig,
          this.stateListener,
          this.uncaughtExceptionHandler);

      this.handlers.forEach(handler ->
          HandlerUtils.registerHandler(easySourcing, handler));

      return easySourcing;
    }

  }
}
