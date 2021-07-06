package io.github.alikelleci.easysourcing.messages.exceptions;

import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.exceptions.exceptions.ExceptionProcessingException;
import io.github.alikelleci.easysourcing.retry.Retry;
import io.github.alikelleci.easysourcing.retry.RetryUtil;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class ExceptionHandler implements Handler<Void> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public ExceptionHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling exception failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling exception failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Void invoke(Object... args) {
    Object command = args[0];
    Metadata metadata = (Metadata) args[1];

    log.debug("Handling exception: {} ({})", command.getClass().getSimpleName(), CommonUtils.getAggregateId(command));

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(command, metadata));
    } catch (Exception e) {
      throw new ExceptionProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Object command, Metadata metadata) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, command);
    } else {
      result = method.invoke(target, command, metadata);
    }
    return null;
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

}
