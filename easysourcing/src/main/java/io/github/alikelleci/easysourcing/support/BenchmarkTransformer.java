package io.github.alikelleci.easysourcing.support;

import io.github.alikelleci.easysourcing.messaging.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
public class BenchmarkTransformer<T extends Message> implements ValueTransformerWithKey<String, T, T> {

  private final AtomicLong counter = new AtomicLong(0);

  @Override
  public void init(ProcessorContext processorContext) {
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        log.info("Processing {} messages/sec", counter.getAndSet(0));
      }
    }, 0, 1000);
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
