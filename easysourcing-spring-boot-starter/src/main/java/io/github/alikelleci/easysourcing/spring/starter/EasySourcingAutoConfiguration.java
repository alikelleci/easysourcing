package io.github.alikelleci.easysourcing.spring.starter;

import io.github.alikelleci.easysourcing.core.EasySourcing;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;

import java.util.List;

@Slf4j
@AutoConfiguration
@ConditionalOnBean(EasySourcing.class)
@EnableConfigurationProperties(EasySourcingProperties.class)
public class EasySourcingAutoConfiguration {

  @Bean
  public EasySourcingBeanPostProcessor easySourcingBeanPostProcessor(@Autowired List<EasySourcing> apps) {
    return new EasySourcingBeanPostProcessor(apps);
  }

  @Bean
  public SmartLifecycle easysourcingLifecycle(List<EasySourcing> apps) {
    return new SmartLifecycle() {
      private volatile boolean running = false;

      @Override
      public void start() {
        apps.forEach(EasySourcing::start);
        running = true;
      }

      @Override
      public void stop() {
        apps.forEach(EasySourcing::stop);
        running = false;
      }

      @Override
      public boolean isRunning() {
        return running;
      }

      @Override
      public int getPhase() {
        return Integer.MAX_VALUE;
      }
    };
  }
}
