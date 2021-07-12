package io.github.alikelleci.easysourcing.messages.events;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

import static io.github.alikelleci.easysourcing.EasySourcingBuilder.OPERATION_MODE;

@Slf4j
public class EventTransformer implements Transformer<String, JsonNode, KeyValue<String, Object>> {

  private final MultiValuedMap<Class<?>, EventHandler> eventHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, Long> redirects;

  public EventTransformer(MultiValuedMap<Class<?>, EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.redirects = context.getStateStore("event-redirects");
  }

  @Override
  public KeyValue<String, Object> transform(String key, JsonNode jsonNode) {
    Object event = JsonUtils.toJavaType(jsonNode);
    if (event == null) {
      return KeyValue.pair(key, null);
    }

    Collection<EventHandler> handlers = eventHandlers.get(event.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return KeyValue.pair(key, null);
    }

    if (redirects.get(key) != null) {
      if (OPERATION_MODE == OperationMode.NORMAL) {
        log.warn("Unprocessed events found, event queued {} ({})", event.getClass().getSimpleName(), key);
        return KeyValue.pair(key, event);
      }

      if (OPERATION_MODE == OperationMode.RETRY) {
        String error = Optional.ofNullable(context.headers().lastHeader("$error"))
            .map(Header::value)
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .orElse(null);

        if (StringUtils.isBlank(error)) {
          log.warn("Unprocessed events found, event queued {} ({})", event.getClass().getSimpleName(), key);
          return KeyValue.pair(key, event);
        }
      }
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    try {
      handlers.stream()
          .sorted(Comparator.comparingInt(EventHandler::getPriority).reversed())
          .forEach(handler ->
              handler.invoke(event, metadata));
    } catch (Exception e) {
      String message = ExceptionUtils.getRootCauseMessage(e);
      context.headers()
          .remove("$error")
          .add("$error", message.getBytes(StandardCharsets.UTF_8));

      log.error("Event not processed: {}", message);
      redirects.put(key, 1L);
      return KeyValue.pair(key, event);
    }

    redirects.put(key, null);
    return KeyValue.pair(key, null);
  }

  @Override
  public void close() {

  }


}
