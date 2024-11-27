package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.easysourcing.messaging.commandhandling.CommandTransformer;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.messaging.eventhandling.EventHandler;
import io.github.alikelleci.easysourcing.messaging.eventhandling.EventTransformer;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.easysourcing.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.easysourcing.messaging.resulthandling.ResultTransformer;
import io.github.alikelleci.easysourcing.support.CustomRocksDbConfig;
import io.github.alikelleci.easysourcing.support.LoggingStateRestoreListener;
import io.github.alikelleci.easysourcing.support.serializer.JsonSerde;
import io.github.alikelleci.easysourcing.util.HandlerUtils;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
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

import static io.github.alikelleci.easysourcing.messaging.Metadata.REPLY_TO;

@Slf4j
@Getter
public class EasySourcing {
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();
  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers = new HashMap<>();
  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers = new ArrayListValuedHashMap<>();
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers = new ArrayListValuedHashMap<>();

  private final Properties streamsConfig;
  private final StateListener stateListener;
  private final StateRestoreListener stateRestoreListener;
  private final StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
  private final ObjectMapper objectMapper;

  private KafkaStreams kafkaStreams;

  protected EasySourcing(Properties streamsConfig,
                         StateListener stateListener,
                         StateRestoreListener stateRestoreListener,
                         StreamsUncaughtExceptionHandler uncaughtExceptionHandler,
                         ObjectMapper objectMapper) {
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.stateRestoreListener = stateRestoreListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
    this.objectMapper = objectMapper;
  }

  public static Builder builder() {
    return new Builder();
  }

  public Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    /*
     * -------------------------------------------------------------
     * SERDES
     * -------------------------------------------------------------
     */

    Serde<Command> commandSerde = new JsonSerde<>(Command.class, objectMapper);
    Serde<Event> eventSerde = new JsonSerde<>(Event.class, objectMapper);
    Serde<Aggregate> snapshotSerde = new JsonSerde<>(Aggregate.class, objectMapper);

    /*
     * -------------------------------------------------------------
     * COMMAND HANDLING
     * -------------------------------------------------------------
     */

    if (!getCommandTopics().isEmpty()) {
      // Snapshot Store
      builder.addStateStore(Stores
          .keyValueStoreBuilder(Stores.persistentKeyValueStore("snapshot-store"), Serdes.String(), snapshotSerde)
          .withLoggingEnabled(Collections.emptyMap()));

      // --> Commands
      KStream<String, Command> commands = builder.stream(getCommandTopics(), Consumed.with(Serdes.String(), commandSerde))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .transformValues(() -> new CommandTransformer(this), "snapshot-store")
          .filter((key, result) -> result != null);

      // Results --> Push
      commandResults
          .mapValues(CommandResult::getCommand)
          .to((key, command, recordContext) -> command.getTopicInfo().value().concat(".results"),
              Produced.with(Serdes.String(), commandSerde));

      // Results --> Push to reply topic
      commandResults
          .mapValues(CommandResult::getCommand)
          .filter((key, command) -> StringUtils.isNotBlank(command.getMetadata().get(REPLY_TO)))
          .to((key, command, recordContext) -> command.getMetadata().get(REPLY_TO),
              Produced.with(Serdes.String(), commandSerde)
                  .withStreamPartitioner((topic, key, value, numPartitions) -> 0));

      // Events --> Push
      commandResults
          .filter((key, result) -> result instanceof Success)
          .mapValues((key, result) -> (Success) result)
          .flatMapValues(Success::getEvents)
          .filter((key, event) -> event != null)
          .to((key, event, recordContext) -> event.getTopicInfo().value(),
              Produced.with(Serdes.String(), eventSerde));
    }

    /*
     * -------------------------------------------------------------
     * EVENT HANDLING
     * -------------------------------------------------------------
     */

    if (!getEventTopics().isEmpty()) {
      // --> Events
      KStream<String, Event> events = builder.stream(getEventTopics(), Consumed.with(Serdes.String(), eventSerde))
          .filter((key, event) -> key != null)
          .filter((key, event) -> event != null)
          .filter((key, event) -> event.getPayload() != null);

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
      KStream<String, Command> results = builder.stream(getResultTopics(), Consumed.with(Serdes.String(), commandSerde))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null)
          .filter((key, command) -> command.getPayload() != null);

      // Results --> Void
      results
          .transformValues(() -> new ResultTransformer(this));
    }


    return builder.build();
  }

  public void start() {
    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("EasySourcing is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    kafkaStreams = new KafkaStreams(topology, streamsConfig);
    setUpListeners();

    log.info("EasySourcing is starting...");
    kafkaStreams.start();
  }

  public void stop() {
    log.info("EasySourcing is shutting down...");
    kafkaStreams.close(Duration.ofSeconds(60));
    log.info("EasySourcing shut down complete.");
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener(this.stateListener);
    kafkaStreams.setGlobalStateRestoreListener(this.stateRestoreListener);
    kafkaStreams.setUncaughtExceptionHandler(this.uncaughtExceptionHandler);

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


  public static class Builder {
    private final List<Object> handlers = new ArrayList<>();

    private Properties streamsConfig;
    private StateListener stateListener;
    private StateRestoreListener stateRestoreListener;
    private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;
    private ObjectMapper objectMapper;

    public Builder registerHandler(Object handler) {
      handlers.add(handler);

      return this;
    }

    public Builder streamsConfig(Properties streamsConfig) {
      this.streamsConfig = streamsConfig;
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
      this.streamsConfig.putIfAbsent(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
      this.streamsConfig.putIfAbsent(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
      this.streamsConfig.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDbConfig.class);
      this.streamsConfig.putIfAbsent(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

      return this;
    }

    public Builder stateListener(StateListener stateListener) {
      this.stateListener = stateListener;
      return this;
    }

    public Builder stateRestoreListener(StateRestoreListener stateRestoreListener) {
      this.stateRestoreListener = stateRestoreListener;
      return this;
    }

    public Builder uncaughtExceptionHandler(StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
      this.uncaughtExceptionHandler = uncaughtExceptionHandler;
      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public EasySourcing build() {
      if (this.stateListener == null) {
        this.stateListener = (newState, oldState) ->
            log.info("State changed from {} to {}", oldState, newState);
      }

      if (this.stateRestoreListener == null) {
        this.stateRestoreListener = new LoggingStateRestoreListener();
      }

      if (this.uncaughtExceptionHandler == null) {
        this.uncaughtExceptionHandler = throwable ->
            StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
      }

      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      EasySourcing easySourcing = new EasySourcing(
          this.streamsConfig,
          this.stateListener,
          this.stateRestoreListener,
          this.uncaughtExceptionHandler,
          this.objectMapper);

      this.handlers.forEach(handler ->
          HandlerUtils.registerHandler(easySourcing, handler));

      return easySourcing;
    }

  }

}
