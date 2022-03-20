package com.github.easysourcing;

import com.github.easysourcing.constants.Topics;
import com.github.easysourcing.messages.commands.CommandStream;
import com.github.easysourcing.messages.events.EventStream;
import com.github.easysourcing.messages.results.ResultStream;
import com.github.easysourcing.messages.snapshots.SnapshotStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class EasySourcing {

  private final Properties streamsConfig;
  private KafkaStreams.StateListener stateListener;
  private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

  private KafkaStreams kafkaStreams;

  protected EasySourcing(Properties streamsConfig) {
    this.streamsConfig = streamsConfig;
  }

  protected EasySourcing(Properties streamsConfig, KafkaStreams.StateListener stateListener, Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  public void start() {
    if (kafkaStreams != null) {
      log.info("EasySourcing already started.");
      return;
    }

    Topology topology = buildTopology();
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
    kafkaStreams.close();
    kafkaStreams = null;
  }

  protected Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    if (CollectionUtils.isNotEmpty(Topics.COMMANDS)) {
      CommandStream commandStream = new CommandStream();
      commandStream.buildStream(builder);
    }

    if (CollectionUtils.isNotEmpty(Topics.EVENTS)) {
      EventStream eventStream = new EventStream();
      eventStream.buildStream(builder);
    }

    if (CollectionUtils.isNotEmpty(Topics.RESULTS)) {
      ResultStream resultStream = new ResultStream();
      resultStream.buildStream(builder);
    }

    if (CollectionUtils.isNotEmpty(Topics.SNAPSHOTS)) {
      SnapshotStream snapshotStream = new SnapshotStream();
      snapshotStream.buildStream(builder);
    }

    return builder.build();
  }

  private void setUpListeners() {
    if (stateListener != null) {
      kafkaStreams.setStateListener(stateListener);
    } else {
      kafkaStreams.setStateListener((newState, oldState) -> {
        log.warn("State changed from {} to {}", oldState, newState);
      });
    }

    if (uncaughtExceptionHandler != null) {
      kafkaStreams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    } else {
      kafkaStreams.setUncaughtExceptionHandler((thread, throwable) -> {
        log.error("EasySourcing will now exit because of the following error: ", throwable);
        System.exit(1);
      });
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("EasySourcing is shutting down...");
      kafkaStreams.close();
    }));
  }
}
