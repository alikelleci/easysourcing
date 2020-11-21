package com.github.easysourcing.messages.results;

import com.github.easysourcing.messages.Handler;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.results.exceptions.ResultProcessingException;
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
public class ResultHandler implements Handler<Void> {

  private final Object target;
  private final Method method;
  private final RetryPolicy<Object> retryPolicy;

  public ResultHandler(Object target, Method method) {
    this.target = target;
    this.method = method;
    this.retryPolicy = RetryUtil.buildRetryPolicyFromAnnotation(method.getAnnotation(Retry.class))
        .onRetry(e -> log.warn("Handling result failed, retrying... ({})", e.getAttemptCount()))
        .onRetriesExceeded(e -> log.error("Handling result failed after {} attempts.", e.getAttemptCount()));
  }

  @Override
  public Void invoke(Object... args) {
    Command command = (Command) args[0];
    ProcessorContext context = (ProcessorContext) args[1];

    if (log.isDebugEnabled()) {
      log.debug("Handling command result: {}", command);
    } else if (log.isInfoEnabled()) {
      log.info("Handling command result: {} ({})", command.getType(), command.getAggregateId());
    }

    try {
      return (Void) Failsafe.with(retryPolicy).get(() -> doInvoke(command, context));
    } catch (Exception e) {
      throw new ResultProcessingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private Void doInvoke(Command command, ProcessorContext context) throws InvocationTargetException, IllegalAccessException {
    Object result;
    if (method.getParameterCount() == 1) {
      result = method.invoke(target, command.getPayload());
    } else {
      result = method.invoke(target, command.getPayload(), command.getMetadata().inject(context));
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
