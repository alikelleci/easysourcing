package com.github.easysourcing.messages.results;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.exceptions.AggregateIdMissingException;
import com.github.easysourcing.messages.exceptions.PayloadMissingException;
import com.github.easysourcing.messages.exceptions.TopicInfoMissingException;
import com.github.easysourcing.messages.results.exceptions.ResultProcessingException;
import com.github.easysourcing.retry.Retry;
import com.github.easysourcing.retry.RetryUtil;
import com.github.easysourcing.utils.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class ResultHandler implements Handler<List<Command>> {

  private Object target;
  private Method method;
  private RetryPolicy<Object> retryPolicy;

  public ResultHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling result failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling result failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public List<Command> invoke(Object... args) {
    Command command = (Command) args[0];

    log.info("Handling result: {}", command);

    try {
      return (List<Command>) Failsafe.with(retryPolicy).get(() -> doInvoke(command));
    } catch (Exception e) {
      throw new ResultProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private List<Command> doInvoke(Command command) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, command.getPayload());
    } else {
      result = method.invoke(target, command.getPayload(), command.getMetadata());
    }
    return createCommands(command, result);
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

  private List<Command> createCommands(Command command, Object result) {
    if (result == null) {
      return new ArrayList<>();
    }

    List<Object> list = new ArrayList<>();
    if (List.class.isAssignableFrom(result.getClass())) {
      list.addAll((List<?>) result);
    } else {
      list.add(result);
    }

    List<Command> commands = list.stream()
        .map(payload -> Command.builder()
            .payload(payload)
            .metadata(MetadataUtils.filterMetadata(command.getMetadata()).toBuilder()
                .entry("$id", UUID.randomUUID().toString())
                .build())
            .build())
        .collect(Collectors.toList());

    commands.forEach(c -> {
      if (c.getPayload() == null) {
        throw new PayloadMissingException("You are trying to dispatch a command without a payload.");
      }
      if (c.getTopicInfo() == null) {
        throw new TopicInfoMissingException("You are trying to dispatch a command without any topic information. Please annotate your command with @TopicInfo.");
      }
      if (c.getAggregateId() == null) {
        throw new AggregateIdMissingException("You are trying to dispatch a command without a proper aggregate identifier. Please annotate your field containing the aggregate identifier with @AggregateId.");
      }
    });

    return commands;
  }

}
