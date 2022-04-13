package com.github.easysourcing.spring.starter;

import com.github.easysourcing.EasySourcing;
import com.github.easysourcing.common.annotations.HandleMessage;
import com.github.easysourcing.utils.HandlerUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;

import java.lang.reflect.Method;
import java.util.List;

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
