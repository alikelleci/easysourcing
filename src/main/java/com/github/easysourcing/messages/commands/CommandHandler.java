package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.commands.exceptions.CommandExecutionException;
import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.messages.exceptions.AggregateIdMismatchException;
import com.github.easysourcing.messages.exceptions.AggregateIdMissingException;
import com.github.easysourcing.messages.exceptions.PayloadMissingException;
import com.github.easysourcing.messages.exceptions.TopicInfoMissingException;
import com.github.easysourcing.retry.Retry;
import com.github.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CommandHandler implements Handler<List<Event>> {

  private Object target;
  private Method method;
  private RetryPolicy<Object> retryPolicy;

  public CommandHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class));

    if (retryPolicy != null) {
      retryPolicy
          .onRetry(e -> log.warn("Handling command failed, retrying... ({})", e.getAttemptCount()))
          .onFailure(e -> log.error("Handling command failed after {} attempts.", e.getAttemptCount()));
    }
  }

  @Override
  public List<Event> invoke(Object... args) {
    Aggregate aggregate = (Aggregate) args[0];
    Command command = (Command) args[1];

    log.info("Handling command: {}", command);

    try {
      if (retryPolicy == null) {
        return doInvoke(aggregate, command);
      }
      return (List<Event>) Failsafe.with(retryPolicy).get(() -> doInvoke(aggregate, command));
    } catch (Exception e) {
      throw new CommandExecutionException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private List<Event> doInvoke(Aggregate aggregate, Command command) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload(), command.getMetadata());
    }
    return createEvents(command, result);
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
    return method.getParameters()[1].getType();
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
        .map(payload -> Event.builder()
            .payload(payload)
            .metadata(command.getMetadata())
            .build())
        .collect(Collectors.toList());

    events.forEach(event -> {
      if (event.getPayload() == null) {
        throw new PayloadMissingException("You are trying to dispatch an event without a payload.");
      }
      if (event.getTopicInfo() == null) {
        throw new TopicInfoMissingException("You are trying to dispatch an event without any topic information. Please annotate your event with @TopicInfo.");
      }
      if (event.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to dispatch an event without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
      if (!StringUtils.equals(event.getAggregateId(), command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + command.getAggregateId() + ", but was " + event.getAggregateId());
      }
    });

    return events;
  }

}
