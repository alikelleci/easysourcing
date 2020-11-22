package com.github.easysourcing.messages.aggregates;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.aggregates.exceptions.AggregateInvocationException;
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
import org.apache.kafka.streams.processor.ProcessorContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

import static com.github.easysourcing.messages.MetadataKeys.ID;

@Slf4j
public class Aggregator implements Handler<Aggregate> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public Aggregator(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Applying event failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Applying event failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Aggregate invoke(Object... args) {
    Aggregate aggregate = (Aggregate) args[0];
    Event event = (Event) args[1];
    ProcessorContext context = (ProcessorContext) args[2];

    if (log.isDebugEnabled()) {
      log.debug("Applying event: {}", event);
    } else if (log.isInfoEnabled()) {
      log.info("Applying event: {} ({})", event.getType(), event.getAggregateId());
    }

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(aggregate, event, context));
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Aggregate doInvoke(Aggregate aggregate, Event event, ProcessorContext context) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata().inject(context));
    }
    return createAggregate(event, result);
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

  private Aggregate createAggregate(Event event, Object result) {
    Aggregate aggregate = Aggregate.builder()
        .payload(result)
        .metadata(event.getMetadata().toBuilder()
            .entry(ID, UUID.randomUUID().toString())
            .build())
        .build();

    if (aggregate.getPayload() == null) {
      throw new PayloadMissingException("You are trying to dispatch an aggregate without a payload.");
    }
    if (aggregate.getTopicInfo() == null) {
      throw new TopicInfoMissingException("You are trying to dispatch an aggregate without any topic information. Please annotate your aggregate with @TopicInfo.");
    }
    if (aggregate.getAggregateId() == null) {
      throw new AggregateIdMissingException("You are trying to dispatch an aggregate without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
    }
    if (!StringUtils.equals(aggregate.getAggregateId(), event.getAggregateId())) {
      throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + event.getAggregateId() + ", but was " + aggregate.getAggregateId());
    }

    return aggregate;
  }

}
