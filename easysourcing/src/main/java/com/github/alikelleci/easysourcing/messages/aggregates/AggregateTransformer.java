package com.github.alikelleci.easysourcing.messages.aggregates;

import com.github.alikelleci.easysourcing.messages.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Map;

@Slf4j
public class AggregateTransformer implements ValueTransformer<Event, Aggregate> {

  private ProcessorContext context;
  private KeyValueStore<String, Aggregate> store;

  private final Map<Class<?>, Aggregator> aggregators;


  public AggregateTransformer(Map<Class<?>, Aggregator> aggregators) {
    this.aggregators = aggregators;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.store = context.getStateStore("snapshot-store");
  }

  @Override
  public Aggregate transform(Event event) {
    Aggregator aggregator = aggregators.get(event.getPayload().getClass());
    if (aggregator == null) {
      return null;
    }

    Aggregate aggregate = store.get(event.getAggregateId());
    aggregate = aggregator.invoke(aggregate, event, context);

    if (aggregate != null) {
      store.put(event.getAggregateId(), aggregate);
    }

    return aggregate;
  }

  @Override
  public void close() {

  }

}
