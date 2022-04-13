package com.github.easysourcing.messaging.eventsourcing;

import com.github.easysourcing.EasySourcing;
import com.github.easysourcing.messaging.eventhandling.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class EventSourcingTransformer implements ValueTransformer<Event, Aggregate> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;
  private KeyValueStore<String, Aggregate> snapshotStore;

  public EventSourcingTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.snapshotStore = context.getStateStore("snapshot-store");
  }

  @Override
  public Aggregate transform(Event event) {
    EventSourcingHandler eventSourcingHandler = easySourcing.getEventSourcingHandlers().get(event.getPayload().getClass());
    if (eventSourcingHandler == null) {
      return null;
    }

    Aggregate aggregate = snapshotStore.get(event.getAggregateId());
    aggregate = eventSourcingHandler.apply(event, aggregate);

    if (aggregate != null) {
      snapshotStore.put(event.getAggregateId(), aggregate);
    }

    return aggregate;
  }

  @Override
  public void close() {

  }

}
