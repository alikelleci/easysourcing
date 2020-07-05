package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.snapshots.exceptions.SnapshotProcessingException;
import com.github.easysourcing.retry.Retry;
import com.github.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

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
    Aggregate snapshot = (Aggregate) args[0];
    ProcessorContext context = (ProcessorContext) args[1];

    log.info("Handling snapshot: {}", snapshot);

    try {
      return (Void) Failsafe.with(retryPolicy).get(() -> doInvoke(snapshot, context));
    } catch (Exception e) {
      throw new SnapshotProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Aggregate snapshot, ProcessorContext context) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, snapshot.getPayload());
    } else {
      result = method.invoke(target, snapshot.getPayload(), snapshot.getMetadata().inject(context));
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
