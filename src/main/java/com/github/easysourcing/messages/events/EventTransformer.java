package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.commands.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;


@Slf4j
public class EventTransformer implements ValueTransformer<Event, List<Command>> {

  private ProcessorContext context;

  private final ConcurrentMap<Class<?>, EventHandler> eventHandlers;

  public EventTransformer(ConcurrentMap<Class<?>, EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
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
    log.debug("Event received: {}", event);

    List<Command> commands = eventHandler.invoke(event);

    context.commit();
    return commands;
  }

  @Override
  public void close() {

  }


}
