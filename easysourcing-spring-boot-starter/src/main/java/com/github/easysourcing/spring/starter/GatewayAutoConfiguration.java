package com.github.easysourcing.spring.starter;

import com.github.easysourcing.GatewayBuilder;
import com.github.easysourcing.messages.MessageGateway;
import com.github.easysourcing.messages.commands.CommandGateway;
import com.github.easysourcing.messages.events.EventGateway;
import com.github.easysourcing.messages.snapshots.SnapshotGateway;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnBean(GatewayBuilder.class)
@EnableConfigurationProperties(EasySourcingProperties.class)
public class GatewayAutoConfiguration {

  @Bean
  public MessageGateway messageGateway(GatewayBuilder builder) {
    return builder
        .messageGateway();
  }

  @Bean
  public CommandGateway commandGateway(GatewayBuilder builder) {
    return builder
        .commandGateway();
  }

  @Bean
  public EventGateway eventGateway(GatewayBuilder builder) {
    return builder
        .eventGateway();
  }

  @Bean
  public SnapshotGateway snapshotGateway(GatewayBuilder builder) {
    return builder
        .snapshotGateway();
  }
}
