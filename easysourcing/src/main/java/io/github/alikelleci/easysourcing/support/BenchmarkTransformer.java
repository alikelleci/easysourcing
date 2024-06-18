package io.github.alikelleci.easysourcing.support;

import io.github.alikelleci.easysourcing.messaging.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class BenchmarkTransformer<T extends Message> implements ValueTransformerWithKey<String, T, T> {
  private final AtomicLong counter = new AtomicLong(0);

  @Override
  public void init(ProcessorContext context) {
    context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp ->
        log.info("Processing {} messages/sec", counter.getAndSet(0)));
  }

  @Override
  public T transform(String s, T t) {
    counter.incrementAndGet();
    return t;
  }

  @Override
  public void close() {

  }

}
