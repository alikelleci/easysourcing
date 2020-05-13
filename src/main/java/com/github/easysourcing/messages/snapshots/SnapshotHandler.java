package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.snapshots.exceptions.SnapshotProcessingException;
import com.github.easysourcing.retry.Retry;
import com.github.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class SnapshotHandler implements Handler<Void> {

  private Object target;
  private Method method;
  private RetryPolicy<Object> retryPolicy;

  public SnapshotHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class));

    if (retryPolicy != null) {
      retryPolicy
          .onRetry(e -> log.warn("Handling snapshot failed, retrying... ({})", e.getAttemptCount()))
          .onFailure(e -> log.error("Handling snapshot failed after {} attempts.", e.getAttemptCount()));
    }
  }

  @Override
  public Void invoke(Object... args) {
    Snapshot snapshot = (Snapshot) args[0];

    log.info("Handling snapshot: {}", snapshot);

    try {
      if (retryPolicy == null) {
        return doInvoke(snapshot);
      }
      return (Void) Failsafe.with(retryPolicy).get(() -> doInvoke(snapshot));
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
