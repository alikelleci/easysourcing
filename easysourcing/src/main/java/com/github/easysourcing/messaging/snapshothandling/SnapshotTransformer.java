package com.github.easysourcing.messaging.snapshothandling;

import com.github.easysourcing.EasySourcing;
import com.github.easysourcing.messaging.eventsourcing.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class SnapshotTransformer implements ValueTransformerWithKey<String, Aggregate, Aggregate> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;

  public SnapshotTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Aggregate transform(String key, Aggregate snapshot) {
    Collection<SnapshotHandler> handlers = easySourcing.getSnapshotHandlers().get(snapshot.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(handlers)) {
      handlers.stream()
          .sorted(Comparator.comparingInt(SnapshotHandler::getPriority).reversed())
          .peek(handler -> handler.setContext(context))
          .forEach(handler ->
              handler.apply(snapshot.toBuilder()
                  .metadata(snapshot.getMetadata().inject(context))
                  .build()));
    }

    return snapshot;
  }

  @Override
  public void close() {

  }


}
