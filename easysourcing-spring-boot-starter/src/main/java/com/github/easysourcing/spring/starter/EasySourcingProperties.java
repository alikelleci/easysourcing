package com.github.easysourcing.spring.starter;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "easysourcing")
public class EasySourcingProperties {
//  private String bootstrapServers;
//  private String applicationId;
}
