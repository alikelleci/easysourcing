package io.github.alikelleci.easysourcing.messaging.eventhandling;

import io.github.alikelleci.easysourcing.common.annotations.Priority;
import io.github.alikelleci.easysourcing.messaging.eventhandling.exceptions.EventProcessingException;
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
public class EventHandler implements Function<Event, Void> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  private ProcessorContext context;

  public EventHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling event failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling event failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Void apply(Event event) {
    log.debug("Handling event: {} ({})", event.getType(), event.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(event));
    } catch (Exception e) {
      throw new EventProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, event.getPayload());
    } else {
      result = method.invoke(target, event.getPayload(), event.getMetadata().inject(context));
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
