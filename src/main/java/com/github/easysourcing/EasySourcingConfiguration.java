package com.github.easysourcing;

import com.github.easysourcing.message.aggregates.annotations.ApplyEvent;
import com.github.easysourcing.message.annotations.TopicInfo;
import com.github.easysourcing.message.commands.annotations.HandleCommand;
import com.github.easysourcing.message.events.annotations.HandleEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.TopicConfig;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.MethodParameterScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.config.TopicBuilder;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

@Slf4j
@ComponentScan("com.github.easysourcing")
@Configuration
public class EasySourcingConfiguration {

  @Value("${easysourcing.replicas:1}")
  private int REPLICAS;

  @Value("${easysourcing.partitions:1}")
  private int PARTITIONS;

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private ConfigurableBeanFactory beanFactory;


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
  public ConcurrentMap<Class<?>, Set<Method>> commandHandlers(Reflections reflections) {
    return reflections.getMethodsAnnotatedWith(HandleCommand.class)
        .stream()
        .filter(method -> method.getParameterCount() == 2 | method.getParameterCount() == 3)
        .collect(Collectors.groupingByConcurrent(method -> method.getParameters()[1].getType(), toSet()));
  }

  @Bean
  public ConcurrentMap<Class<?>, Set<Method>> eventHandlers(Reflections reflections) {
    return reflections.getMethodsAnnotatedWith(HandleEvent.class)
        .stream()
        .filter(method -> method.getParameterCount() == 1 | method.getParameterCount() == 2)
        .collect(Collectors.groupingByConcurrent(method -> method.getParameters()[0].getType(), toSet()));
  }

  @Bean
  public ConcurrentMap<Class<?>, Set<Method>> aggregateHandlers(Reflections reflections) {
    return reflections.getMethodsAnnotatedWith(ApplyEvent.class)
        .stream()
        .filter(method -> method.getParameterCount() == 2 | method.getParameterCount() == 3)
        .filter(method -> method.getReturnType() == method.getParameters()[0].getType())
        .collect(Collectors.groupingByConcurrent(method -> method.getParameters()[1].getType(), toSet()));
  }

  @Bean
  public Set<String> commandsTopics(ConcurrentMap<Class<?>, Set<Method>> commandHandlers) {
    Set<TopicInfo> topicInfos = commandHandlers.keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .collect(Collectors.toSet());

    topicInfos.stream()
        .map(topicInfo -> TopicBuilder.name(topicInfo.value())
            .partitions(PARTITIONS)
            .replicas(REPLICAS)
            .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
            .build())
        .forEach(newTopic -> beanFactory.registerSingleton(newTopic.name(), newTopic));

    return topicInfos.stream()
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  @Bean
  public Set<String> eventsTopics(ConcurrentMap<Class<?>, Set<Method>> eventHandlers, ConcurrentMap<Class<?>, Set<Method>> aggregateHandlers) {
    Set<TopicInfo> topicInfos = Stream.of(eventHandlers.keySet(), aggregateHandlers.keySet())
        .flatMap(Collection::stream)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .collect(Collectors.toSet());

    topicInfos.stream()
        .map(topicInfo -> TopicBuilder.name(topicInfo.value())
            .partitions(PARTITIONS)
            .replicas(REPLICAS)
            .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
            .build())
        .forEach(newTopic -> beanFactory.registerSingleton(newTopic.name(), newTopic));

    return topicInfos.stream()
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }
}
