package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.aggregates.Aggregator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Map;
import java.util.Optional;

@Slf4j
public class EventSourcingTransformer implements ValueTransformer<Event, Aggregate> {

  private ProcessorContext context;
  private KeyValueStore<String, ValueAndTimestamp<Aggregate>> store;

  private final Map<Class<?>, Aggregator> aggregators;


  public EventSourcingTransformer(Map<Class<?>, Aggregator> aggregators) {
    this.aggregators = aggregators;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = (KeyValueStore<String, ValueAndTimestamp<Aggregate>>) context.getStateStore("snapshot-store");
  }

  @Override
  public Aggregate transform(Event event) {
    Aggregator aggregator = aggregators.get(event.getPayload().getClass());
    if (aggregator == null) {
      return null;
    }

    Aggregate aggregate = loadAggregate(event.getAggregateId());
    aggregate = aggregator.invoke(aggregate, event, context);

    store.put(event.getAggregateId(), ValueAndTimestamp
        .make(aggregate, context.timestamp()));

    return aggregate;
  }

  @Override
  public void close() {

  }

  private Aggregate loadAggregate(String id) {
    ValueAndTimestamp<Aggregate> record = store.get(id);
    return Optional.ofNullable(record)
        .map(ValueAndTimestamp::value)
        .orElse(null);
  }

}
