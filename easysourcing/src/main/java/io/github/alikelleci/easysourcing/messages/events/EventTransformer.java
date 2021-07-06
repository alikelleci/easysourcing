package io.github.alikelleci.easysourcing.messages.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.upcasters.Upcaster;
import io.github.alikelleci.easysourcing.messages.upcasters.annotations.Upcast;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;


public class EventTransformer implements ValueTransformer<JsonNode, Void> {

  private ProcessorContext context;

  private final MultiValuedMap<String, Upcaster> upcasters;
  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;

  public EventTransformer(MultiValuedMap<String, Upcaster> upcasters, MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.upcasters = upcasters;
    this.eventHandlers = eventHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(JsonNode jsonEvent) {
    upcastIfNeeded(jsonEvent);

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


  private JsonNode upcastIfNeeded(JsonNode jsonNode) {
    String className = Optional.ofNullable(jsonNode)
        .map(node -> node.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return null;
    }

    Collection<Upcaster> handlers = upcasters.get(className);
    if (CollectionUtils.isEmpty(handlers)) {
      return jsonNode;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .forEach(handler -> {
          ObjectNode result = (ObjectNode) handler.invoke(jsonNode, metadata);
          ((ObjectNode) jsonNode).removeAll();
          ((ObjectNode) jsonNode).setAll(result);
        });

    return jsonNode;
  }

}
