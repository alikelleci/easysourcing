package com.easysourcing.api;

import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan("com.example.easysourcing")
public abstract class EasySourcingAbstractConfiguration {

  public abstract String scanPackages();


  @Bean
  public Reflections reflections() {
    return new Reflections(scanPackages(),
        new TypeAnnotationsScanner(),
        new SubTypesScanner(),
        new MethodAnnotationsScanner(),
        new MethodParameterScanner()
    );
  }

}
