package com.github.easysourcing.messaging.eventsourcing;

import com.github.easysourcing.messaging.eventsourcing.exceptions.AggregateInvocationException;
import com.github.easysourcing.messaging.eventhandling.Event;
import com.github.easysourcing.common.exceptions.AggregateIdMismatchException;
import com.github.easysourcing.common.exceptions.AggregateIdMissingException;
import com.github.easysourcing.common.exceptions.PayloadMissingException;
import com.github.easysourcing.common.exceptions.TopicInfoMissingException;
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
import java.util.function.BiFunction;

import static com.github.easysourcing.messaging.Metadata.ID;

@Slf4j
public class EventSourcingHandler implements BiFunction<Event, Aggregate, Aggregate> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  private ProcessorContext context;

  public EventSourcingHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Applying event failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Applying event failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Aggregate apply(Event event, Aggregate aggregate) {
    log.debug("Applying event: {} ({})", event.getType(), event.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(aggregate, event));
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Aggregate doInvoke(Aggregate aggregate, Event event) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata().inject(context));
    }
    return createState(event, result);
  }

  private Aggregate createState(Event event, Object result) {
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

  public void setContext(ProcessorContext context) {
    this.context = context;
  }
}
