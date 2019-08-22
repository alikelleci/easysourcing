package com.example.easysourcing.message;


import com.example.easysourcing.message.commands.annotations.HandleCommand;
import com.example.easysourcing.message.events.annotations.HandleEvent;
import com.example.easysourcing.message.snapshots.Snapshotable;
import com.example.easysourcing.message.snapshots.annotations.ApplyEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.reflections.Reflections;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.reflections.ReflectionUtils.getConstructors;

@Slf4j
@Component
public class MessageStream {

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private AdminClient adminClient;

  @Autowired
  private Reflections reflections;


  @Bean
  public KStream<String, Message> eventStreamBuilder(StreamsBuilder builder) throws ExecutionException, InterruptedException {
    // Collect all unique topics
    Set<String> topics = getAllSubscribedTopicsForCommands();
    topics.addAll(getAllSubscribedTopicsForEvents());
    topics.addAll(getAllSubscribedTopicsForEventSourcing());
    if (topics.isEmpty()) {
      return null;
    }
    createTopics(topics);


    // 1.  Read message stream
    KStream<String, Message> stream = builder
        .stream(topics, Consumed.with(Serdes.String(), new JsonSerde<>(Message.class)))
        .filter((key, message) -> message != null)
        .filter((key, message) -> message.getPayload() != null);


    // 2. Split stream into Commands and Events
    KStream<String, Message>[] messageBranches = stream.branch(
        (key, message) -> message.getType() == MessageType.Command,
        (key, message) -> message.getType() == MessageType.Event
    );


    // 3. Process Commands
    messageBranches[0]
        .mapValues(Message::getPayload)
        .filter((key, payload) -> getCommandHandler(payload) != null)
        .mapValues(this::invokeCommandHandler)
        .filter((key, result) -> result != null)
        .mapValues((key, result) -> Message.builder()
            .type(MessageType.Event)
            .payload(result)
            .build())
        .to((key, message, recordContext) -> message.getPayload().getClass().getPackage().getName().concat("-eventstore"),
            Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));


    // 4. Process Events, split stream
    KStream<String, Object>[] eventBranches = messageBranches[1]
        .mapValues(Message::getPayload)
        .branch(
            (key, payload) -> getEventHandler(payload) != null,
            (key, payload) -> getEventSourcingHandler(payload) != null
        );


    // Events that will be processed by @HandleEvent method
    eventBranches[0]
        .mapValues(this::invokeEventHandler)
        .filter((key, results) -> results != null && !results.isEmpty())
        .flatMapValues(results -> results)
        .filter((key, result) -> result != null)
        .mapValues((key, result) -> Message.builder()
            .type(MessageType.Command)
            .payload(result)
            .build())
        .to((key, message, recordContext) -> message.getPayload().getClass().getPackage().getName().concat("-eventstore"),
            Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));


    // Events that will be processed by @ApplyEvent method
    eventBranches[1]
        .groupByKey()
        .aggregate(
            () -> null,
            (key, payload, currentAggregate) -> {
              if (currentAggregate == null) {
                currentAggregate = (Snapshotable) instantiateClazz(getEventSourcingHandler(payload).getDeclaringClass());
              }
              return invokeEventSourcingHandler(payload, currentAggregate);
            },
            Materialized
                .<String, Snapshotable, KeyValueStore<Bytes, byte[]>>as("snapshots")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(Snapshotable.class)));

    return stream;
  }


  public void createTopics(Set<String> topics) throws ExecutionException, InterruptedException {
    List<String> existingTopics = adminClient.listTopics().listings().get()
        .stream()
        .map(TopicListing::name)
        .collect(Collectors.toList());

    List<NewTopic> newTopics = new ArrayList<>();
    topics.forEach(topic ->
        newTopics.add(new NewTopic(topic, 6, (short) 1)));

    // filter existing topics and create new topics
    newTopics.removeIf(newTopic -> existingTopics.contains(newTopic.name()));
    adminClient.createTopics(newTopics).all().get();
  }


  //
  // Commands
  //

  private Set<String> getAllSubscribedTopicsForCommands() {
    return applicationContext.getBeansWithAnnotation(Component.class).values()
        .stream()
        .flatMap(bean -> Arrays.stream(bean.getClass().getMethods()))
        .filter(method -> method.isAnnotationPresent(HandleCommand.class))
        .filter(method -> method.getReturnType() != Void.TYPE)
        .filter(method -> method.getParameterCount() == 1)
        .map(method -> method.getParameters()[0].getType())
        .map(aClass -> aClass.getPackage().getName().concat("-eventstore"))
        .collect(Collectors.toSet());
  }


  private <T, V> V invokeCommandHandler(T payload) {
    Method methodToInvoke = getCommandHandler(payload);
    if (methodToInvoke != null) {
      Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
      try {
        return (V) MethodUtils.invokeExactMethod(bean, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No command-handler method found for command {}", payload.getClass().getSimpleName());
    return null;
  }


  private <T> Method getCommandHandler(T payload) {
    return applicationContext.getBeansWithAnnotation(Component.class).values()
        .stream()
        .flatMap(bean -> Arrays.stream(bean.getClass().getMethods()))
        .filter(method -> method.isAnnotationPresent(HandleCommand.class))
        .filter(method -> method.getReturnType() != Void.TYPE)
        .filter(method -> method.getParameterCount() == 1)
        .filter(method -> method.getParameters()[0].getType() == payload.getClass())
        .findFirst()
        .orElse(null);
  }


  //
  // Events
  //

  private Set<String> getAllSubscribedTopicsForEvents() {
    return applicationContext.getBeansWithAnnotation(Component.class).values()
        .stream()
        .flatMap(bean -> Arrays.stream(bean.getClass().getMethods()))
        .filter(method -> method.isAnnotationPresent(HandleEvent.class))
        .filter(method -> method.getReturnType() == List.class)
        .filter(method -> method.getParameterCount() == 1)
        .map(method -> method.getParameters()[0].getType())
        .map(aClass -> aClass.getPackage().getName().concat("-eventstore"))
        .collect(Collectors.toSet());
  }

  private <T, V> List<V> invokeEventHandler(T payload) {
    Method methodToInvoke = getEventHandler(payload);
    if (methodToInvoke != null) {
      Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
      try {
        return (List<V>) MethodUtils.invokeExactMethod(bean, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
    return Collections.emptyList();
  }

  private <T> Method getEventHandler(T payload) {
    return applicationContext.getBeansWithAnnotation(Component.class).values()
        .stream()
        .flatMap(bean -> Arrays.stream(bean.getClass().getMethods()))
        .filter(method -> method.isAnnotationPresent(HandleEvent.class))
        .filter(method -> method.getReturnType() == List.class)
        .filter(method -> method.getParameterCount() == 1)
        .filter(method -> method.getParameters()[0].getType() == payload.getClass())
        .findFirst()
        .orElse(null);
  }


  //
  // Event Sourcing
  //

  private Set<String> getAllSubscribedTopicsForEventSourcing() {
    return reflections.getSubTypesOf(Snapshotable.class).stream()
        .flatMap(aClass -> Arrays.stream(aClass.getMethods()))
        .filter(method -> method.isAnnotationPresent(ApplyEvent.class))
        .filter(method -> method.getReturnType() == method.getDeclaringClass())
        .filter(method -> method.getParameterCount() == 1)
        .map(method -> method.getParameters()[0].getType())
        .map(aClass -> aClass.getPackage().getName().concat("-eventstore"))
        .collect(Collectors.toSet());
  }

  private <T> Snapshotable invokeEventSourcingHandler(T payload, Snapshotable bean) {
    Method methodToInvoke = getEventSourcingHandler(payload);
    if (methodToInvoke != null) {
      try {
        return (Snapshotable) MethodUtils.invokeExactMethod(bean, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
    return null;
  }


  private <T> Method getEventSourcingHandler(T payload) {
    return reflections.getSubTypesOf(Snapshotable.class)
        .stream()
        .flatMap(aClass -> Arrays.stream(aClass.getMethods()))
        .filter(method -> method.isAnnotationPresent(ApplyEvent.class))
        .filter(method -> method.getReturnType() == method.getDeclaringClass())
        .filter(method -> method.getParameterCount() == 1)
        .filter(method -> method.getParameters()[0].getType() == payload.getClass())
        .findFirst()
        .orElse(null);
  }


  private <T> T instantiateClazz(Class<T> tClass) {
    Constructor constructor = getConstructors(tClass).stream()
        .findFirst()
        .orElseThrow(() -> new RuntimeException("No constructor found!"));

    Parameter[] parameters = constructor.getParameters();
    Object[] arguments = new Object[parameters.length];
    for (int i = 0; i < parameters.length; i++) {
      arguments[i] = null;
    }
    return (T) BeanUtils.instantiateClass(constructor, arguments);
  }
}
