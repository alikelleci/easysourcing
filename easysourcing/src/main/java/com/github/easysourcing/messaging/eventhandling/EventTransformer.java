package com.github.easysourcing.messaging.eventhandling;

import com.github.easysourcing.EasySourcing;
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
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Event transform(String key, Event event) {
    Collection<EventHandler> handlers = easySourcing.getEventHandlers().get(event.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(handlers)) {
      handlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .forEach(handler ->
              handler.apply(event.toBuilder()
                  .metadata(event.getMetadata().inject(context))
                  .build()));
    }

    return event;
  }

  @Override
  public void close() {

  }


}
