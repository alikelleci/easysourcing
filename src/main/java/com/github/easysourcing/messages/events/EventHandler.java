package com.github.easysourcing.messages.events;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.events.exceptions.EventProcessingException;
import com.github.easysourcing.messages.exceptions.AggregateIdMissingException;
import com.github.easysourcing.retry.Retry;
import com.github.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class EventHandler implements Handler<List<Command>> {

  private Object target;
  private Method method;
  private RetryPolicy<Object> retryPolicy;

  public EventHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class));

    if (retryPolicy != null) {
      retryPolicy
          .onRetry(e -> log.warn("Handling event failed, retrying... ({})", e.getAttemptCount()))
          .onFailure(e -> log.error("Handling event failed after {} attempts.", e.getAttemptCount()));
    }
  }

  @Override
  public List<Command> invoke(Object... args) {
    Event event = (Event) args[0];

    log.info("Handling event: {}", event);

    try {
      if (retryPolicy == null) {
        return doInvoke(event);
      }
      return (List<Command>) Failsafe.with(retryPolicy).get(() -> doInvoke(event));
    } catch (Exception e) {
      throw new EventProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private List<Command> doInvoke(Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, event.getPayload());
    } else {
      result = method.invoke(target, event.getPayload(), event.getMetadata());
    }
    return createCommands(event, result);
  }

  @Override
  public Object getTarget() {
    return target;
  }

  @Override
  public Method getMethod() {
    return method;
  }

  @Override
  public Class<?> getType() {
    return method.getParameters()[0].getType();
  }

  private List<Command> createCommands(Event event, Object result) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Command> commands = list.stream()
        .map(payload -> Command.builder()
            .uuid(UUID.randomUUID().toString())
            .reference(event.getUuid())
            .payload(payload)
            .metadata(event.getMetadata())
            .build())
        .collect(Collectors.toList());

    commands.forEach(command -> {
      if (command.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to dispatch a command without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
    });

    return commands;
  }
}
