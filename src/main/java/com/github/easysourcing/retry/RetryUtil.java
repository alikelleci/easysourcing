package com.github.easysourcing.retry;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
@UtilityClass
public class RetryUtil {

  public RetryPolicy<Object> buildRetryPolicyFromAnnotation(Retry retry) {
    if (retry == null || retry.attempts() == 0) {
      return null;
    }

    RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
        .withMaxRetries(retry.attempts());

    if (retry.backoff() == Backoff.EXPONENTIAL) {
      retryPolicy
          .withBackoff(retry.delay(), Integer.MAX_VALUE, ChronoUnit.MILLIS);
    } else {
      retryPolicy
          .withDelay(Duration.ofMillis(retry.delay()));
    }

    Class<? extends Throwable>[] exceptions = retry.exceptions();
    for (Class<? extends Throwable> exception : exceptions) {
      retryPolicy.handleIf(throwable -> exception.isAssignableFrom(ExceptionUtils.getRootCause(throwable).getClass()));
    }

    return retryPolicy;
  }
}
