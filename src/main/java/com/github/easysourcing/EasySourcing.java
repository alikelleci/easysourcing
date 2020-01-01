package com.github.easysourcing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

@Slf4j
public class EasySourcing {

  private Config config;

  private Topology topology;
  private KafkaStreams kafkaStreams;

  private boolean running = false;

  protected EasySourcing(Config config, Topology topology) {
    this.config = config;
    this.topology = topology;
  }

  public void start() {
    if (running) {
      log.warn("EasySourcing already started.");
      return;
    }

    this.kafkaStreams = new KafkaStreams(topology, config.streamsConfig());

    addShutdownHook();
    setUpListeners();

    log.info("EasySourcing is starting.");
    kafkaStreams.start();
    this.running = true;
  }

  public void stop() {
    if (!running) {
      log.warn("EasySourcing already stopped.");
      return;
    }

    if (kafkaStreams != null) {
      log.info("EasySourcing is shutting down.");
      kafkaStreams.close();
      kafkaStreams = null;
      running = false;
    }
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener((newState, oldState) -> log.warn("State changed from {} to {}", oldState, newState));
    kafkaStreams.setUncaughtExceptionHandler((t, e) -> log.error("Exception handler triggered ", e));
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(EasySourcing.this::stop));
  }
}
