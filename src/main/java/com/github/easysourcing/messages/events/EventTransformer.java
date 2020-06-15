package com.github.easysourcing.messages.events;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.concurrent.ConcurrentMap;


public class EventTransformer implements ValueTransformer<Event, Void> {

  private ProcessorContext context;

  private final ConcurrentMap<Class<?>, EventHandler> eventHandlers;
  private final boolean frequentCommits;

  public EventTransformer(ConcurrentMap<Class<?>, EventHandler> eventHandlers, boolean frequentCommits) {
    this.eventHandlers = eventHandlers;
    this.frequentCommits = frequentCommits;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Event event) {
    EventHandler eventHandler = eventHandlers.get(event.getPayload().getClass());
    if (eventHandler == null) {
      return null;
    }

    Void v = eventHandler.invoke(event, context);

    if (frequentCommits) {
      context.commit();
    }
    return v;
  }

  @Override
  public void close() {

  }


}
