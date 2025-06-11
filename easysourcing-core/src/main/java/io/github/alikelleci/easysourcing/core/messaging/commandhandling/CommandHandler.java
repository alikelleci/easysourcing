package io.github.alikelleci.easysourcing.core.messaging.commandhandling;

import io.github.alikelleci.easysourcing.core.common.CommonParameterResolver;
import io.github.alikelleci.easysourcing.core.common.annotations.AggregateRoot;
import io.github.alikelleci.easysourcing.core.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.easysourcing.core.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.PayloadMissingException;
import io.github.alikelleci.easysourcing.core.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.AggregateState;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

@Slf4j
@Getter
public class CommandHandler implements BiFunction<AggregateState, Command, List<Event>>, CommonParameterResolver {

  private final Object handler;
  private final Method method;

  private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

  private FixedKeyProcessorContext<?, ?> context;

  public CommandHandler(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
  }

  @Override
  public List<Event> apply(AggregateState state, Command command) {
    try {
      validate(command.getPayload());
      Object result = invokeHandler(handler, state, command);
      return createEvents(command, result);
    } catch (Exception e) {
      throw new CommandExecutionException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object invokeHandler(Object handler, AggregateState state, Command command) throws InvocationTargetException, IllegalAccessException {
    Object[] args = new Object[method.getParameterCount()];
    Parameter[] parameters = method.getParameters();

    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      if (i == 0) {
        args[i] = command.getPayload();
      } else if (parameter.getType().isAnnotationPresent(AggregateRoot.class)) {
        args[i] = state != null ? state.getPayload() : null;
      } else {
        args[i] = resolve(parameter, command, context);
      }
    }

    // Invoke the method
    return method.invoke(handler, args);
  }

  private List<Event> createEvents(Command command, Object result) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Event> events = list.stream()
        .filter(Objects::nonNull)
        .map(payload -> Event.builder()
            .payload(payload)
            .metadata(Metadata.builder()
                .addAll(command.getMetadata())
                .remove(Metadata.RESULT)
                .remove(Metadata.FAILURE)
                .build())
            .build())
        .toList();

    events.forEach(event -> {
      if (event.getPayload() == null) {
        throw new PayloadMissingException("You are trying to publish an event without a payload.");
      }
      if (event.getTopicInfo() == null) {
        throw new TopicInfoMissingException("You are trying to publish an event without any topic information. Please annotate your event with @TopicInfo.");
      }
      if (event.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to publish an event without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
      if (!StringUtils.equals(event.getAggregateId(), command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match for event " + event.getType() +". Expected " + command.getAggregateId() + ", but was " + event.getAggregateId());
      }
    });

    return events;
  }

  private void validate(Object payload) {
    Set<ConstraintViolation<Object>> violations = validator.validate(payload);
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

  public void setContext(FixedKeyProcessorContext context) {
    this.context = context;
  }
}
