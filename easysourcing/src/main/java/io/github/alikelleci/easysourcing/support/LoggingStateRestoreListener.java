package io.github.alikelleci.easysourcing.support;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LoggingStateRestoreListener implements StateRestoreListener {

  private final Map<TopicPartition, Stats> stores = new HashMap<>();

  public LoggingStateRestoreListener() {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(() ->
        stores.forEach((topicPartition, stats) -> {
          if (stats.getCurrentOffset() < stats.getEndingOffset()) {
            double progressPercentage = ((double) stats.getCurrentOffset() / stats.getEndingOffset()) * 100;
            log.debug("State restoration in progress: store={}, partition={}, progress={}%", stats.getStoreName(), topicPartition.partition(), ((int) progressPercentage));
          }
        }), 0, 10, TimeUnit.SECONDS);

    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdown));
  }

  @Override
  public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
//    log.debug("State restoration started: topic={}, partition={}, store={}, startingOffset={}, endingOffset={}", topicPartition.topic(), topicPartition.partition(), storeName, startingOffset, endingOffset);
    log.debug("State restoration started: store={}, partition={}, startingOffset={}, endingOffset={}", storeName, topicPartition.partition(), startingOffset, endingOffset);

    stores.put(topicPartition, Stats.builder()
        .storeName(storeName)
        .currentOffset(startingOffset)
        .endingOffset(endingOffset)
        .build());
  }

  @Override
  public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
//    log.debug("State restoration in progress: topic={}, partition={}, store={}, numRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, numRestored);
    stores.get(topicPartition).setCurrentOffset(batchEndOffset);
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
//    log.debug("State restoration ended: topic={}, partition={}, store={}, totalRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
    log.debug("State restoration ended: store={}, partition={}, totalRestored={}", storeName, topicPartition.partition(), totalRestored);
    stores.remove(topicPartition);
  }


  @Data
  @Builder(toBuilder = true)
  static class Stats {
    private String storeName;
    private long endingOffset;
    private long currentOffset;
  }
}
