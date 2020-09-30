package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.Handler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class EventTransformer implements ValueTransformer<Event, Void> {

  private ProcessorContext context;

  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;
  private final boolean frequentCommits;

  public EventTransformer(MultiValuedMap<Class<?>, EventHandler> eventHandlers, boolean frequentCommits) {
    this.eventHandlers = eventHandlers;
    this.frequentCommits = frequentCommits;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Event event) {
    Collection<EventHandler> handlers = eventHandlers.get(event.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted((Comparator.comparingInt(Handler::getOrder)))
        .forEach(handler ->
            handler.invoke(event, context));

    if (frequentCommits) {
      context.commit();
    }
    return null;
  }

  @Override
  public void close() {

  }


}
