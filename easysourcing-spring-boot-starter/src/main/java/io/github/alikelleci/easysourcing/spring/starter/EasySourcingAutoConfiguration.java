package io.github.alikelleci.easysourcing.spring.starter;

import io.github.alikelleci.easysourcing.EasySourcing;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
@ConditionalOnBean(EasySourcing.class)
@EnableConfigurationProperties(EasySourcingProperties.class)
public class EasySourcingAutoConfiguration {

  @Autowired
  private ApplicationContext applicationContext;

  @Bean
  public EasySourcingBeanPostProcessor easySourcingBeanPostProcessor(@Autowired List<EasySourcing> apps) {
    return new EasySourcingBeanPostProcessor(apps);
  }

  @EventListener
  public void onApplicationEvent(ApplicationReadyEvent event) {
    if (event.getApplicationContext().equals(this.applicationContext)) {
      Map<String, EasySourcing> apps = event.getApplicationContext().getBeansOfType(EasySourcing.class);
      apps.values().forEach(EasySourcing::start);
    }
  }
}
