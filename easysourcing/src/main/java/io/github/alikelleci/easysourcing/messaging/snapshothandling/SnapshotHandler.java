package io.github.alikelleci.easysourcing.messaging.snapshothandling;

import io.github.alikelleci.easysourcing.common.annotations.Priority;
import io.github.alikelleci.easysourcing.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.easysourcing.messaging.snapshothandling.exceptions.SnapshotProcessingException;
import io.github.alikelleci.easysourcing.retry.Retry;
import io.github.alikelleci.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

@Slf4j
public class SnapshotHandler implements Function<Aggregate, Void> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  private ProcessorContext context;

  public SnapshotHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling snapshot failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling snapshot failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Void apply(Aggregate snapshot) {
    log.debug("Handling snapshot: {} ({})", snapshot.getType(), snapshot.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(snapshot));
    } catch (Exception e) {
      throw new SnapshotProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Aggregate snapshot) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, snapshot.getPayload());
    } else {
      result = method.invoke(target, snapshot.getPayload(), snapshot.getMetadata().inject(context));
    }
    return null;
  }

  public Method getMethod() {
    return method;
  }

  public int getPriority() {
    return Optional.ofNullable(method.getAnnotation(Priority.class))
        .map(Priority::value)
        .orElse(0);
  }

  public void setContext(ProcessorContext context) {
    this.context = context;
  }
}
