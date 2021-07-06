package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.exceptions.CommandExecutionException;
import io.github.alikelleci.easysourcing.retry.Retry;
import io.github.alikelleci.easysourcing.retry.RetryUtil;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class CommandHandler implements Handler<List<Object>> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  private final Validator validator;

  public CommandHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling command failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling command failed after {} attempts.", e.getAttemptCount()));

    this.validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Override
  public List<Object> invoke(Object... args) {
    Object command = args[0];
    Object snapshot = args[1];
    Metadata metadata = (Metadata) args[2];

    log.debug("Handling command: {} ({})", command.getClass().getSimpleName(), CommonUtils.getAggregateId(command));

    try {
      validate(command);
      return Failsafe.with(retryPolicy).get(() -> doInvoke(command, snapshot, metadata));
    } catch (Exception e) {
      throw new CommandExecutionException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private List<Object> doInvoke(Object command, Object snapshot, Metadata metadata) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, command, snapshot);
    } else {
      result = method.invoke(target, command, snapshot, metadata);
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
    return method.getParameters()[0].getType();
  }

  private List<Object> createEvents(Object command, Object result) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> events = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      events.addAll((List<?>) result);
    } else {
      events.add(result);
    }

    events.forEach(event -> {
      CommonUtils.validatePayload(event);
      if (!StringUtils.equals(CommonUtils.getAggregateId(event), CommonUtils.getAggregateId(command))) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + CommonUtils.getAggregateId(command) + ", but was " + CommonUtils.getAggregateId(event));
      }
    });

    return events;
  }

  private void validate(Object command) {
    Set<ConstraintViolation<Object>> violations = validator.validate(command);
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

}
