package com.github.easysourcing.spring.starter;

import com.github.easysourcing.EasySourcing;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

@Slf4j
@Configuration
@ConditionalOnBean(EasySourcing.class)
@EnableConfigurationProperties(EasySourcingProperties.class)
public class EasySourcingAutoConfiguration {

  @Autowired
  private ApplicationContext applicationContext;

  @Bean
  public EasySourcingBeanPostProcessor easySourcingBeanPostProcessor(EasySourcing easySourcing) {
    return new EasySourcingBeanPostProcessor(easySourcing);
  }

  @EventListener
  public void onApplicationEvent(ApplicationReadyEvent event) {
    if (event.getApplicationContext().equals(this.applicationContext)) {
      EasySourcing easySourcing = event.getApplicationContext().getBean(EasySourcing.class);
      easySourcing.start();
    }
  }
}
