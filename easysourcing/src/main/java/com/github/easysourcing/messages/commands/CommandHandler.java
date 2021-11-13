package com.github.easysourcing.messages.commands;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.commands.exceptions.CommandExecutionException;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.processor.ProcessorContext;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.github.easysourcing.messages.Metadata.ID;
import static com.github.easysourcing.messages.commands.CommandResult.Failure;
import static com.github.easysourcing.messages.commands.CommandResult.Success;

@Slf4j
public class CommandHandler implements Handler<CommandResult> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  private final Validator validator;

  public CommandHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling command failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling command failed after {} attempts.", e.getAttemptCount()));

    this.validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Override
  public CommandResult invoke(Object... args) {
    Aggregate aggregate = (Aggregate) args[0];
    Command command = (Command) args[1];
    ProcessorContext context = (ProcessorContext) args[2];

    log.debug("Handling command: {} ({})", command.getType(), command.getAggregateId());

    try {
      validate(command);
      return Failsafe.with(retryPolicy).get(() -> doInvoke(aggregate, command, context));
    } catch (Exception e) {
      Throwable throwable = ExceptionUtils.getRootCause(e);
      String message = ExceptionUtils.getRootCauseMessage(e);

      if (throwable instanceof ValidationException) {
        log.debug("Command rejected: {}", message);
        return Failure.builder()
            .command(command)
            .message(message)
            .build();
      }
      throw new CommandExecutionException(message, throwable);
    }
  }

  private CommandResult doInvoke(Aggregate aggregate, Command command, ProcessorContext context) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 2) {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload());
    } else {
      result = method.invoke(target, aggregate != null ? aggregate.getPayload() : null, command.getPayload(), command.getMetadata().inject(context));
    }
    return createResult(command, result);
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

  private CommandResult createResult(Command command, Object result) {
    if (result == null) {
      return null;
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Event> events = list.stream()
        .map(payload -> Event.builder()
            .payload(payload)
            .metadata(command.getMetadata().toBuilder()
                .entry(ID, UUID.randomUUID().toString())
                .build())
            .build())
        .collect(Collectors.toList());

    events.forEach(event -> {
      if (event.getPayload() == null) {
        throw new PayloadMissingException("You are trying to dispatch an event without a payload.");
      }
      if (event.getTopicInfo() == null) {
        throw new TopicInfoMissingException("You are trying to dispatch an event without any topic information. Please annotate your event with @TopicInfo.");
      }
      if (event.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to dispatch an event without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
      if (!StringUtils.equals(event.getAggregateId(), command.getAggregateId())) {
        throw new AggregateIdMismatchException("Aggregate identifier does not match. Expected " + command.getAggregateId() + ", but was " + event.getAggregateId());
      }
    });

    return Success.builder()
        .command(command)
        .events(events)
        .build();
  }

  private void validate(Command command) {
    Set<ConstraintViolation<Object>> violations = validator.validate(command.getPayload());
    if (!CollectionUtils.isEmpty(violations)) {
      throw new ConstraintViolationException(violations);
    }
  }

}