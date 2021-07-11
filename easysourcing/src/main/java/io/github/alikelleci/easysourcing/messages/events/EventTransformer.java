package io.github.alikelleci.easysourcing.messages.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class EventTransformer implements Transformer<String, JsonNode, KeyValue<String, Void>> {

  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;
  private ProcessorContext context;

  public EventTransformer(MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public KeyValue<String, Void> transform(String key, JsonNode jsonNode) {
    Object event = JsonUtils.toJavaType(jsonNode);
    if (event == null) {
      return KeyValue.pair(key, null);
    }

    Collection<EventHandler> handlers = eventHandlers.get(event.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return KeyValue.pair(key, null);
    }

    log.debug("Handling event: {} ({})", event.getClass().getSimpleName(), key);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(event, metadata));

    return KeyValue.pair(key, null);
  }

  @Override
  public void close() {

  }


}
