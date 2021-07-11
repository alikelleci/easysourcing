package io.github.alikelleci.easysourcing.messages.eventsourcing;

import io.github.alikelleci.easysourcing.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.eventsourcing.exceptions.AggregateInvocationException;
import io.github.alikelleci.easysourcing.retry.Retry;
import io.github.alikelleci.easysourcing.retry.RetryUtil;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class EventSourcingHandler implements Handler<Object> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public EventSourcingHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Applying event failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Applying event failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Object invoke(Object... args) {
    Object event = args[0];
    Object snapshot = args[1];
    Metadata metadata = (Metadata) args[2];

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(event, snapshot, metadata));
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Object doInvoke(Object event, Object snapshot, Metadata metadata) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, event, snapshot);
    } else {
      result = method.invoke(target, event, snapshot, metadata);
    }
    return createSnapshot(event, result);
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

  private Object createSnapshot(Object event, Object result) {
    CommonUtils.validatePayload(result);
    if (!StringUtils.equals(CommonUtils.getAggregateId(result), CommonUtils.getAggregateId(event))) {
      throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + CommonUtils.getAggregateId(event) + ", but was " + CommonUtils.getAggregateId(result));
    }

    return result;
  }

}
