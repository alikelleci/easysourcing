package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.aggregates.Aggregate;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class SnapshotTransformer implements ValueTransformer<Aggregate, Void> {

  private ProcessorContext context;

  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers;
  private final boolean frequentCommits;

  public SnapshotTransformer(MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers, boolean frequentCommits) {
    this.snapshotHandlers = snapshotHandlers;
    this.frequentCommits = frequentCommits;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Aggregate snapshot) {
    Collection<SnapshotHandler> handlers = snapshotHandlers.get(snapshot.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted((Comparator.comparingInt(Handler::getOrder)))
        .forEach(handler ->
            handler.invoke(snapshot, context));

    if (frequentCommits) {
      context.commit();
    }
    return null;
  }

  @Override
  public void close() {

  }


}
