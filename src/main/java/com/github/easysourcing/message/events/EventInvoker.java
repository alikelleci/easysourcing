package com.github.easysourcing.message.events;

import com.github.easysourcing.message.commands.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


@Slf4j
public class EventInvoker implements ValueTransformer<Event, List<Command>> {

  private ProcessorContext context;
  private EventService eventService;

  public EventInvoker(EventService eventService) {
    this.eventService = eventService;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public List<Command> transform(Event event) {
    Method eventHandler = eventService.getEventHandler(event);
    if (eventHandler == null) {
      return new ArrayList<>();
    }
    log.debug("Event received: {}", event);

    List<Command> commands = eventService.invokeEventHandler(eventHandler, event);

    context.commit();
    return commands;
  }

  @Override
  public void close() {

  }


}
