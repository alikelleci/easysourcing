package io.github.alikelleci.easysourcing.messages.eventsourcing;

import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

@Slf4j
public class EventSourcingTransformer implements ValueTransformer<Event, Snapshot> {

  private ProcessorContext context;
  private KeyValueStore<String, Snapshot> store;

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
  public Snapshot transform(Event event) {
    EventSourcingHandler eventSourcingHandler = this.eventSourcingHandlers.get(event.getPayload().getClass());
    if (eventSourcingHandler == null) {
      return null;
    }

    Snapshot snapshot = store.get(event.getAggregateId());
    snapshot = eventSourcingHandler.invoke(snapshot, event, context);

    if (snapshot != null) {
      store.put(event.getAggregateId(), snapshot);
    }

    return snapshot;
  }

  @Override
  public void close() {

  }

}
