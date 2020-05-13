package com.github.easysourcing.messages.snapshots;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.concurrent.ConcurrentMap;


public class SnapshotTransformer implements ValueTransformer<Snapshot, Void> {

  private ProcessorContext context;

  private final ConcurrentMap<Class<?>, SnapshotHandler> snapshotHandlers;
  private final boolean frequentCommits;

  public SnapshotTransformer(ConcurrentMap<Class<?>, SnapshotHandler> snapshotHandlers, boolean frequentCommits) {
    this.snapshotHandlers = snapshotHandlers;
    this.frequentCommits = frequentCommits;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Snapshot snapshot) {
    SnapshotHandler snapshotHandler = snapshotHandlers.get(snapshot.getPayload().getClass());
    if (snapshotHandler == null) {
      return null;
    }

    Void v = snapshotHandler.invoke(snapshot);

    if (frequentCommits) {
      context.commit();
    }
    return v;
  }

  @Override
  public void close() {

  }


}
