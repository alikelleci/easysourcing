package com.easysourcing.api;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@ComponentScan("com.easysourcing.api")
@Configuration
public class EasySourcingConfiguration {

  @Autowired
  private ApplicationContext applicationContext;

  private String getHostPackageName() {
    Map<String, Object> annotatedBeans = applicationContext.getBeansWithAnnotation(SpringBootApplication.class);
    return annotatedBeans.isEmpty() ? null : annotatedBeans.values().toArray()[0].getClass().getPackage().getName();
  }


  @Bean
  public Reflections reflections() {
    return new Reflections(getHostPackageName(),
        new TypeAnnotationsScanner(),
        new SubTypesScanner(),
        new MethodAnnotationsScanner(),
        new MethodParameterScanner()
    );
  }
}
