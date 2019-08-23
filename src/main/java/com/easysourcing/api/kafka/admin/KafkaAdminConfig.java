package com.easysourcing.api.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

  @Value(" ${spring.kafka.bootstrap-servers}")
  private String BOOTSTRAP_SERVERS;

  @Bean
  public KafkaAdmin admin() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

    return new KafkaAdmin(properties);
  }

  @Bean
  public AdminClient adminClient() {
    return AdminClient.create(admin().getConfig());
  }

}
