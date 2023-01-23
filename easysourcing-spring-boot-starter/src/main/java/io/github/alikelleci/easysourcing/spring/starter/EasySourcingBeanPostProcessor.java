package io.github.alikelleci.easysourcing.spring.starter;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.util.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.List;
import java.util.stream.Collectors;

public class EasySourcingBeanPostProcessor implements BeanPostProcessor {

  private final List<EasySourcing> apps;

  public EasySourcingBeanPostProcessor(List<EasySourcing> apps) {
    this.apps = apps.stream()
        .filter(eventify -> eventify.getCommandHandlers().isEmpty())
        .filter(eventify -> eventify.getEventSourcingHandlers().isEmpty())
        .filter(eventify -> eventify.getResultHandlers().isEmpty())
        .filter(eventify -> eventify.getEventHandlers().isEmpty())
        .collect(Collectors.toList());
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    apps.forEach(easySourcing ->
        HandlerUtils.registerHandler(easySourcing, bean));

    return bean;
  }

}
