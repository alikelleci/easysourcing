package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.constants.Handlers;
import com.github.easysourcing.messages.aggregates.Aggregate;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class SnapshotTransformer implements ValueTransformer<Aggregate, Void> {

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Aggregate snapshot) {
    Collection<SnapshotHandler> handlers = Handlers.SNAPSHOT_HANDLERS.get(snapshot.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(SnapshotHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(snapshot, context));

    return null;
  }

  @Override
  public void close() {

  }


}
