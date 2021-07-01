package io.github.alikelleci.easysourcing.spring.starter;

import io.github.alikelleci.easysourcing.EasySourcingBuilder;
import io.github.alikelleci.easysourcing.util.HandlerUtils;
import io.github.alikelleci.easysourcing.common.annotations.HandleMessage;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Method;
import java.util.List;

public class EasySourcingBeanPostProcessor implements BeanPostProcessor {

  private final EasySourcingBuilder builder;

  public EasySourcingBeanPostProcessor(EasySourcingBuilder easySourcingBuilder) {
    this.builder = easySourcingBuilder;
  }

  @Override
  public Object postProcessBeforeInitialization(final Object bean, final String beanName) {
    return bean;
  }

  @Override
  public Object postProcessAfterInitialization(final Object bean, final String beanName) {
    List<Method> methods = HandlerUtils.findMethodsWithAnnotation(bean.getClass(), HandleMessage.class);
    if (CollectionUtils.isNotEmpty(methods)) {
      builder.registerHandler(bean);
    }
    return bean;
  }

}
