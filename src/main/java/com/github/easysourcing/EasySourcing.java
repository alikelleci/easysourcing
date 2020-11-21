package com.github.easysourcing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.time.Duration;

@Slf4j
public class EasySourcing {

  private final Config config;

  private final Topology topology;
  private KafkaStreams kafkaStreams;

  protected EasySourcing(Config config, Topology topology) {
    this.config = config;
    this.topology = topology;
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

    this.kafkaStreams = new KafkaStreams(topology, config.streamsConfig());
    setUpListeners();

    if (config.isRebuildLocalState()) {
      log.warn("Rebuild local state is set to 'true'. Please restart the application with 'rebuildLocalState=false' when the rebuilding process is finished.");
      log.warn("Deleting local state...");
      kafkaStreams.cleanUp();
    }

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
    });

    kafkaStreams.setUncaughtExceptionHandler((thread, throwable) -> {
      log.error("EasySourcing will now exit because of the following error: ", throwable);
      System.exit(1);
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("EasySourcing is shutting down...");
      kafkaStreams.close(Duration.ofMillis(1000));
    }));
  }

}
