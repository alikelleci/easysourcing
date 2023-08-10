package io.github.alikelleci.easysourcing.messaging.eventsourcing;

import io.github.alikelleci.easysourcing.messaging.Metadata;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.exceptions.AggregateInvocationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.BiFunction;

import static io.github.alikelleci.easysourcing.messaging.Metadata.EVENT_ID;
import static io.github.alikelleci.easysourcing.messaging.Metadata.ID;

@Slf4j
public class EventSourcingHandler implements BiFunction<Aggregate, Event, Aggregate> {

  private final Object target;
  private final Method method;

  private ProcessorContext context;

  public EventSourcingHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public Aggregate apply(Aggregate aggregate, Event event) {
    log.debug("Applying event: {} ({})", event.getType(), event.getAggregateId());

    try {
      return doInvoke(aggregate, event);
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Aggregate doInvoke(Aggregate aggregate, Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata().inject(context));
    }
    return createState(event, result);
  }

  private Aggregate createState(Event event, Object result) {
    if (result == null) {
      return null;
    }

    return Aggregate.builder()
        .payload(result)
        .metadata(Metadata.builder()
            .addAll(event.getMetadata())
            .add(EVENT_ID, event.getMetadata().get(ID))
            .build())
        .build();
  }

  public Method getMethod() {
    return method;
  }

  public void setContext(ProcessorContext context) {
    this.context = context;
  }
}
