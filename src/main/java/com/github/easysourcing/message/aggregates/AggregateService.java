package com.github.easysourcing.message.aggregates;

import com.github.easysourcing.message.aggregates.exceptions.AggregateInvocationException;
import com.github.easysourcing.message.events.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;


@Slf4j
@Service
public class AggregateService {

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private ConcurrentMap<Class<?>, Set<Method>> aggregateHandlers;


  public Method getAggregateHandler(Event event) {
    return CollectionUtils.emptyIfNull(aggregateHandlers.get(event.getPayload().getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  public Aggregate invokeAggregateHandler(Method methodToInvoke, Aggregate aggregate, Event event) {
    Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
    Object result;
    try {
      if (methodToInvoke.getParameterCount() == 2) {
        result = methodToInvoke.invoke(bean, aggregate != null ? aggregate.getPayload() : null, event.getPayload());
      } else {
        result = methodToInvoke.invoke(bean, aggregate != null ? aggregate.getPayload() : null, event.getPayload(), event.getMetadata());
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new AggregateInvocationException(e.getCause().getMessage(), e.getCause());
    }

    return Aggregate.builder()
        .payload(result)
        .metadata(event.getMetadata())
        .build();
  }

}
