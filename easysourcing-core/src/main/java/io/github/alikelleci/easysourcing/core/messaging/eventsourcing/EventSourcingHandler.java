package io.github.alikelleci.easysourcing.core.messaging.eventsourcing;

import io.github.alikelleci.easysourcing.core.common.CommonParameterResolver;
import io.github.alikelleci.easysourcing.core.common.annotations.AggregateRoot;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.exceptions.AggregateInvocationException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.function.BiFunction;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.EVENT_ID;
import static io.github.alikelleci.easysourcing.core.messaging.Metadata.ID;

@Slf4j
@Getter
public class EventSourcingHandler implements BiFunction<AggregateState, Event, AggregateState>, CommonParameterResolver {

  private final Object handler;
  private final Method method;

  private FixedKeyProcessorContext<?, ?> context;

  public EventSourcingHandler(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public AggregateState apply(AggregateState state, Event event) {
    try {
      Object result = invokeHandler(handler, state, event);
      return createState(event, result);
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, AggregateState state, Event event) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = event.getPayload();
      } else if (parameter.getType().isAnnotationPresent(AggregateRoot.class)) {
        args[i] = state != null ? state.getPayload() : null;
      } else {
        args[i] = resolve(parameter, event, context);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
  }

  private AggregateState createState(Event event, Object result) {
    if (result == null) {
      return null;
    }

    return AggregateState.builder()
        .payload(result)
        .metadata(Metadata.builder()
            .addAll(event.getMetadata())
            .add(EVENT_ID, event.getMetadata().get(ID))
            .build())
        .build();
  }

  public void setContext(FixedKeyProcessorContext<?, ?> context) {
    this.context = context;
  }
}
