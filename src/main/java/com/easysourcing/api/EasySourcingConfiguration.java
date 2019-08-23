package com.easysourcing.api;

import com.easysourcing.api.message.commands.annotations.HandleCommand;
import com.easysourcing.api.message.events.annotations.HandleEvent;
import com.easysourcing.api.message.snapshots.Snapshotable;
import com.easysourcing.api.message.snapshots.annotations.ApplyEvent;
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

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

  @Bean
  public Map<Class<?>, Set<Method>> commandHandlers(Reflections reflections) {
    return reflections.getMethodsAnnotatedWith(HandleCommand.class)
        .stream()
        .filter(method -> method.getReturnType() != Void.TYPE)
        .filter(method -> method.getParameterCount() == 1)
        .collect(Collectors.groupingBy(method -> method.getParameters()[0].getType(), toSet()));
  }

  @Bean
  public Map<Class<?>, Set<Method>> eventHandlers(Reflections reflections) {
    return reflections.getMethodsAnnotatedWith(HandleEvent.class)
        .stream()
        .filter(method -> method.getReturnType() == List.class)
        .filter(method -> method.getParameterCount() == 1)
        .collect(Collectors.groupingBy(method -> method.getParameters()[0].getType(), toSet()));
  }

  @Bean
  public Map<Class<?>, Set<Method>> eventSourcingHandlers(Reflections reflections) {
    return reflections.getSubTypesOf(Snapshotable.class)
        .stream()
        .flatMap(aClass -> Arrays.stream(aClass.getMethods()))
        .filter(method -> method.isAnnotationPresent(ApplyEvent.class))
        .filter(method -> method.getReturnType() == method.getDeclaringClass())
        .filter(method -> method.getParameterCount() == 1)
        .collect(Collectors.groupingBy(method -> method.getParameters()[0].getType(), toSet()));
  }
}
