package com.github.easysourcing.retry;

import com.github.easysourcing.messages.exceptions.AggregateIdMismatchException;
import com.github.easysourcing.messages.exceptions.AggregateIdMissingException;
import com.github.easysourcing.messages.exceptions.PayloadMissingException;
import com.github.easysourcing.messages.exceptions.TopicInfoMissingException;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.validation.ValidationException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@UtilityClass
public class RetryUtil {

  public RetryPolicy<Object> buildRetryPolicyFromAnnotation(Retry retry) {
    if (retry == null || retry.attempts() == 0) {
      return new RetryPolicy<>()
          .abortOn(Exception.class);
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

    List<Class<? extends Throwable>> included = new ArrayList<>();
    CollectionUtils.addAll(included, retry.exceptions());

    List<Class<? extends Throwable>> excluded = Arrays.asList(
        ValidationException.class,
        AggregateIdMissingException.class,
        AggregateIdMismatchException.class,
        PayloadMissingException.class,
        TopicInfoMissingException.class
    );

    included.forEach(aClass ->
        retryPolicy
            .handleIf(throwable ->
                ClassUtils.isAssignable(ExceptionUtils.getRootCause(throwable).getClass(), aClass)));

    excluded.forEach(aClass ->
        retryPolicy
            .abortOn(throwable ->
                ClassUtils.isAssignable(ExceptionUtils.getRootCause(throwable).getClass(), aClass)));

    return retryPolicy;
  }
}
