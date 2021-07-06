package io.github.alikelleci.easysourcing.messages.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;


public class EventTransformer implements ValueTransformer<JsonNode, Void> {

  private ProcessorContext context;

  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;

  public EventTransformer(MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(JsonNode jsonEvent) {
    Object event = JsonUtils.toJavaType(jsonEvent);
    if (event == null) {
      return null;
    }

    Collection<EventHandler> handlers = eventHandlers.get(event.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(event, metadata));

    return null;
  }

  @Override
  public void close() {

  }


}
