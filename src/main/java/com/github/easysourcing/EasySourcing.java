package com.github.easysourcing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.Properties;

@Slf4j
public class EasySourcing {

  private final Topology topology;
  private final Properties streamsConfig;
  private StateListener stateListener;
  private UncaughtExceptionHandler uncaughtExceptionHandler;

  private KafkaStreams kafkaStreams;

  protected EasySourcing(Topology topology, Properties streamsConfig) {
    this.topology = topology;
    this.streamsConfig = streamsConfig;
  }

  protected EasySourcing(Topology topology, Properties streamsConfig, StateListener stateListener, UncaughtExceptionHandler uncaughtExceptionHandler) {
    this.topology = topology;
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  public void start() {
    if (kafkaStreams != null) {
      log.info("EasySourcing already started.");
      return;
    }

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
    kafkaStreams.close(Duration.ofMillis(1000));
    kafkaStreams = null;
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener((newState, oldState) -> {
      log.warn("State changed from {} to {}", oldState, newState);
      if (stateListener != null) {
        stateListener.onChange(newState, oldState);
      }
    });

    kafkaStreams.setUncaughtExceptionHandler((thread, throwable) -> {
      log.error("EasySourcing will now exit because of the following error: ", throwable);
      if (uncaughtExceptionHandler != null) {
        uncaughtExceptionHandler.uncaughtException(thread, throwable);
      }
      System.exit(1);
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("EasySourcing is shutting down...");
      kafkaStreams.close(Duration.ofMillis(1000));
    }));
  }

  public KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }


  @FunctionalInterface
  public interface StateListener {
    void onChange(KafkaStreams.State newState, KafkaStreams.State oldState);
  }

  @FunctionalInterface
  public interface UncaughtExceptionHandler {
    void uncaughtException(Thread thread, Throwable throwable);
  }
}
