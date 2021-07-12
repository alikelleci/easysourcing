package io.github.alikelleci.easysourcing.messages.eventsourcing;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class EventSourcingTransformer implements ValueTransformerWithKey<String, JsonNode, Object> {

  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, JsonNode> snapshots;
  private KeyValueStore<String, Long> redirects;

  public EventSourcingTransformer(Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.snapshots = context.getStateStore("snapshots");
    this.redirects = context.getStateStore("redirects");
  }

  @Override
  public Object transform(String key, JsonNode jsonNode) {
    Object event = JsonUtils.toJavaType(jsonNode);
    if (event == null) {
      return null;
    }

    EventSourcingHandler eventSourcingHandler = eventSourcingHandlers.get(event.getClass());
    if (eventSourcingHandler == null) {
      return null;
    }

    Object snapshot = Optional.ofNullable(snapshots.get(key))
        .map(JsonUtils::toJavaType)
        .orElse(null);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    snapshot = eventSourcingHandler.invoke(event, snapshot, metadata);

    Optional.ofNullable(snapshot)
        .map(JsonUtils::toJsonNode)
        .ifPresent(node -> snapshots.put(key, node));

    redirects.delete(key);
    return snapshot;
  }

  @Override
  public void close() {

  }

}
