package io.github.alikelleci.easysourcing.messages.eventsourcing;

import io.github.alikelleci.easysourcing.common.annotations.Revision;
import io.github.alikelleci.easysourcing.common.exceptions.AggregateIdMismatchException;
import io.github.alikelleci.easysourcing.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.easysourcing.common.exceptions.PayloadMissingException;
import io.github.alikelleci.easysourcing.common.exceptions.TopicInfoMissingException;
import io.github.alikelleci.easysourcing.messages.Handler;
import io.github.alikelleci.easysourcing.messages.events.Event;
import io.github.alikelleci.easysourcing.messages.eventsourcing.exceptions.AggregateInvocationException;
import io.github.alikelleci.easysourcing.messages.snapshots.Snapshot;
import io.github.alikelleci.easysourcing.retry.Retry;
import io.github.alikelleci.easysourcing.retry.RetryUtil;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.messages.MetadataKeys.ID;
import static io.github.alikelleci.easysourcing.messages.MetadataKeys.REVISION;

@Slf4j
public class EventSourcingHandler implements Handler<Snapshot> {

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
  public Snapshot invoke(Object... args) {
    Snapshot snapshot = (Snapshot) args[0];
    Event event = (Event) args[1];
    ProcessorContext context = (ProcessorContext) args[2];

    log.debug("Applying event: {} ({})", event.getPayloadType(), event.getAggregateId());

    try {
      return Failsafe.with(retryPolicy).get(() -> doInvoke(snapshot, event, context));
    } catch (Exception e) {
      throw new AggregateInvocationException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Snapshot doInvoke(Snapshot snapshot, Event event, ProcessorContext context) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, snapshot != null ? snapshot.getPayload() : null, event.getPayload());
    } else {
      result = method.invoke(target, snapshot != null ? snapshot.getPayload() : null, event.getPayload(), event.getMetadata().injectContext(context));
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
    return method.getParameters()[1].getType();
  }

  private Snapshot createSnapshot(Event event, Object result) {
    Snapshot snapshot = Snapshot.builder()
        .payload(result)
        .metadata(event.getMetadata()
            .add(ID, UUID.randomUUID().toString())
            .add(REVISION, Optional.ofNullable(AnnotationUtils.findAnnotation(result.getClass(), Revision.class))
                .map(Revision::value)
                .orElse(0)))
        .build();

    if (snapshot.getPayload() == null) {
      throw new PayloadMissingException("You are trying to dispatch snapshot without a payload.");
    }
    if (snapshot.getTopicInfo() == null) {
      throw new TopicInfoMissingException("You are trying to dispatch a snapshot without any topic information. Please annotate your aggregate with @TopicInfo.");
    }
    if (snapshot.getAggregateId() == null) {
      throw new AggregateIdMissingException("You are trying to dispatch a snapshot without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
    }
    if (!StringUtils.equals(snapshot.getAggregateId(), event.getAggregateId())) {
      throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + event.getAggregateId() + ", but was " + snapshot.getAggregateId());
    }

    return snapshot;
  }

}
