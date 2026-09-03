package io.github.alikelleci.easysourcing.spring.starter;

import io.github.alikelleci.easysourcing.core.EasySourcing;
import io.github.alikelleci.easysourcing.core.common.annotations.HandleMessage;
import io.github.alikelleci.easysourcing.core.util.AnnotationUtils;
import io.github.alikelleci.easysourcing.core.util.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.util.Arrays;
import java.util.List;

public class EasySourcingBeanPostProcessor implements BeanPostProcessor {

  private final List<EasySourcing> apps;

  public EasySourcingBeanPostProcessor(List<EasySourcing> apps) {
    this.apps = apps.stream()
        .filter(easySourcing -> easySourcing.getCommandHandlers().isEmpty())
        .filter(easySourcing -> easySourcing.getEventSourcingHandlers().isEmpty())
        .filter(easySourcing -> easySourcing.getResultHandlers().isEmpty())
        .filter(easySourcing -> easySourcing.getEventHandlers().isEmpty())
        .toList();
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    if (isHandler(bean)) {
      apps.forEach(easySourcing ->
          HandlerUtils.registerHandler(easySourcing, bean));
    }
    return bean;
  }

  private boolean isHandler(Object bean) {
    return Arrays.stream(bean.getClass().getDeclaredMethods())
        .anyMatch(method -> AnnotationUtils.findAnnotation(method, HandleMessage.class) != null);
  }
}
