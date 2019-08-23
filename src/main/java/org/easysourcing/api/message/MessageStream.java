package org.easysourcing.api.message;


import org.easysourcing.api.message.snapshots.Snapshot;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
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
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.reflections.ReflectionUtils.getConstructors;

@Slf4j
@Component
public class MessageStream {

  @Value("${spring.kafka.streams.application-id}")
  private String APPLICATION_ID;

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private AdminClient adminClient;

  @Autowired
  private Map<Class<?>, Set<Method>> commandHandlers;

  @Autowired
  private Map<Class<?>, Set<Method>> eventHandlers;

  @Autowired
  private Map<Class<?>, Set<Method>> eventSourcingHandlers;


  @PostConstruct
  private void initTopics() throws ExecutionException, InterruptedException {
    List<String> existingTopics = adminClient.listTopics().listings().get()
        .stream()
        .map(TopicListing::name)
        .collect(Collectors.toList());

    List<String> topics = Arrays.asList("events." + APPLICATION_ID);
    List<NewTopic> newTopics = new ArrayList<>();
    topics.forEach(topic ->
        newTopics.add(new NewTopic(topic, 6, (short) 1)));

    // filter existing topics and create new topics
    newTopics.removeIf(newTopic -> existingTopics.contains(newTopic.name()));
    adminClient.createTopics(newTopics).all().get();
  }

  @Bean
  public KStream<String, Message> eventStreamBuilder(StreamsBuilder builder) throws ExecutionException, InterruptedException {
    // 1.  Read message stream
    KStream<String, Message> stream = builder
        .stream(Pattern.compile("events.(.*)"), Consumed.with(Serdes.String(), new JsonSerde<>(Message.class)))
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
        .to("events." + APPLICATION_ID, Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));


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
        .to("events." + APPLICATION_ID, Produced.with(Serdes.String(), new JsonSerde<>(Message.class)));


    // Events that will be processed by @ApplyEvent method
    eventBranches[1]
        .groupByKey()
        .aggregate(
            () -> null,
            (key, payload, snapshot) -> {
              if (snapshot == null) {
                Object aggregate = instantiateClazz(getEventSourcingHandler(payload).getDeclaringClass());
                snapshot = Snapshot.builder().data(aggregate).build();
              }
              Object aggregate = invokeEventSourcingHandler(payload, snapshot.getData());
              return snapshot.toBuilder().data(aggregate).build();
            },
            Materialized
                .<String, Snapshot, KeyValueStore<Bytes, byte[]>>as("snapshots")
                .withKeySerde(Serdes.String())
                .withValueSerde(new JsonSerde<>(Snapshot.class)));

    return stream;
  }


  //
  // Commands
  //

  public <C> Method getCommandHandler(C payload) {
    return CollectionUtils.emptyIfNull(commandHandlers.get(payload.getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <C, E> E invokeCommandHandler(C payload) {
    Method methodToInvoke = getCommandHandler(payload);
    if (methodToInvoke != null) {
      Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
      try {
        return (E) MethodUtils.invokeExactMethod(bean, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No command-handler method found for command {}", payload.getClass().getSimpleName());
    return null;
  }


  //
  // Events
  //

  public <E> Method getEventHandler(E payload) {
    return CollectionUtils.emptyIfNull(eventHandlers.get(payload.getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <E, C> List<C> invokeEventHandler(E payload) {
    Method methodToInvoke = getEventHandler(payload);
    if (methodToInvoke != null) {
      Object bean = applicationContext.getBean(methodToInvoke.getDeclaringClass());
      try {
        return (List<C>) MethodUtils.invokeExactMethod(bean, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
    return Collections.emptyList();
  }


  //
  // Event Sourcing
  //

  public <E> Method getEventSourcingHandler(E payload) {
    return CollectionUtils.emptyIfNull(eventSourcingHandlers.get(payload.getClass()))
        .stream()
        .findFirst()
        .orElse(null);
  }

  private <E, A> A invokeEventSourcingHandler(E payload, A aggregate) {
    Method methodToInvoke = getEventSourcingHandler(payload);
    if (methodToInvoke != null) {
      try {
        return (A) MethodUtils.invokeExactMethod(aggregate, methodToInvoke.getName(), payload);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    log.debug("No event-handler method found for event {}", payload.getClass().getSimpleName());
    return null;
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
