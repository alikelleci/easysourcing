package io.github.alikelleci.easysourcing.messages.snapshots;

import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.snapshots.exceptions.SnapshotProcessingException;
import io.github.alikelleci.easysourcing.retry.Retry;
import io.github.alikelleci.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class SnapshotHandler implements Handler<Void> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public SnapshotHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling snapshot failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling snapshot failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Void invoke(Object... args) {
    Snapshot snapshot = (Snapshot) args[0];

    log.debug("Handling snapshot: {} ({})", snapshot.getPayloadType(), snapshot.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(snapshot));
    } catch (Exception e) {
      throw new SnapshotProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Snapshot snapshot) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, snapshot.getPayload());
    } else {
      result = method.invoke(target, snapshot.getPayload(), snapshot.getMetadata());
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
