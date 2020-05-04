package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.commands.Command;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;


public class EventTransformer implements ValueTransformer<Event, List<Command>> {

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
  public List<Command> transform(Event event) {
    EventHandler eventHandler = eventHandlers.get(event.getPayload().getClass());
    if (eventHandler == null) {
      return new ArrayList<>();
    }

    List<Command> commands = eventHandler.invoke(event);

    if (frequentCommits) {
      context.commit();
    }
    return commands;
  }

  @Override
  public void close() {

  }


}
