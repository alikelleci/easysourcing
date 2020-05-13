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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
public class Aggregator implements Handler<Aggregate> {

  private Object target;
  private Method method;
  private RetryPolicy<Object> retryPolicy;

  public Aggregator(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class));

    if (retryPolicy != null) {
      retryPolicy
          .onRetry(e -> log.warn("Applying event failed, retrying... ({})", e.getAttemptCount()))
          .onFailure(e -> log.error("Applying event failed after {} attempts.", e.getAttemptCount()));
    }
  }

  @Override
  public Aggregate invoke(Object... args) {
    Aggregate aggregate = (Aggregate) args[0];
    Event event = (Event) args[1];

    log.info("Applying event: {}", event);

    try {
      if (retryPolicy == null) {
        return doInvoke(aggregate, event);
      }
      return (Aggregate) Failsafe.with(retryPolicy).get(() -> doInvoke(aggregate, event));
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Aggregate doInvoke(Aggregate aggregate, Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata());
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
        .metadata(event.getMetadata())
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
