package io.github.alikelleci.easysourcing.core.support;

import io.github.alikelleci.easysourcing.core.messaging.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class BenchmarkProcessor<T extends Message> implements FixedKeyProcessor<String, T, T> {

  private FixedKeyProcessorContext<String, T> context;
  private final AtomicLong counter = new AtomicLong(0);

  @Override
  public void init(FixedKeyProcessorContext<String, T> context) {
    this.context = context;
    this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp ->
        log.info("Processing {} messages/sec", counter.getAndSet(0)));
  }

  @Override
  public void process(FixedKeyRecord<String, T> fixedKeyRecord) {
    counter.incrementAndGet();
    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {

  }
}
