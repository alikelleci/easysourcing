package io.github.alikelleci.easysourcing.messages.eventsourcing;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class EventSourcingTransformer implements ValueTransformer<JsonNode, Object> {

  private ProcessorContext context;
  private KeyValueStore<String, JsonNode> store;

  private final Map<Class<?>, EventSourcingHandler> eventSourcingHandlers;


  public EventSourcingTransformer(Map<Class<?>, EventSourcingHandler> eventSourcingHandlers) {
    this.eventSourcingHandlers = eventSourcingHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = context.getStateStore("snapshot-store");
  }

  @Override
  public Object transform(JsonNode jsonEvent) {
    Object event = JsonUtils.toJavaType(jsonEvent);
    if (event == null) {
      return null;
    }

    EventSourcingHandler eventSourcingHandler = eventSourcingHandlers.get(event.getClass());
    if (eventSourcingHandler == null) {
      return null;
    }

    String key = CommonUtils.getAggregateId(event);

    Object snapshot = Optional.ofNullable(store.get(key))
        .map(JsonUtils::toJavaType)
        .orElse(null);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    snapshot = eventSourcingHandler.invoke(event, snapshot, metadata);

    Optional.ofNullable(snapshot)
        .map(JsonUtils::toJsonNode)
        .ifPresent(jsonNode -> store.put(key, jsonNode));

    return snapshot;
  }

  @Override
  public void close() {

  }

}
