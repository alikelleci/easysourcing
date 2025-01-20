package io.github.alikelleci.easysourcing.messaging.eventhandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventTransformer implements ValueTransformerWithKey<String, Event, Event> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;

  public EventTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public Event transform(String key, Event event) {
    Collection<EventHandler> eventHandlers = easySourcing.getEventHandlers().get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(eventHandlers)) {
      eventHandlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .peek(handler -> handler.setContext(context))
          .forEach(handler ->
              handler.apply(event));
    }

    return event;
  }

  @Override
  public void close() {

  }


}
