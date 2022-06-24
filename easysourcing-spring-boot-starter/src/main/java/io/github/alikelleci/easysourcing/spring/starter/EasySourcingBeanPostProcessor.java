package io.github.alikelleci.easysourcing.spring.starter;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.utils.HandlerUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

public class EasySourcingBeanPostProcessor implements BeanPostProcessor {

  private final EasySourcing easySourcing;

  public EasySourcingBeanPostProcessor(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    HandlerUtils.registerHandler(easySourcing, bean);
    return bean;
  }

}
