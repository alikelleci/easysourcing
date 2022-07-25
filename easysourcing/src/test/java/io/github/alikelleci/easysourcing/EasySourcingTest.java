package io.github.alikelleci.easysourcing;

import io.github.alikelleci.easysourcing.example.domain.CustomerCommand;
import io.github.alikelleci.easysourcing.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerResultHandler;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.support.serializer.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.easysourcing.messaging.Metadata.FAILURE;
import static io.github.alikelleci.easysourcing.messaging.Metadata.ID;
import static io.github.alikelleci.easysourcing.messaging.Metadata.RESULT;
import static io.github.alikelleci.easysourcing.messaging.Metadata.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class EasySourcingTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commandsTopic;
  private TestOutputTopic<String, Command> commandResultsTopic;
  private TestOutputTopic<String, Event> eventsTopic;

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

    EasySourcing easySourcing = EasySourcing.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .registerHandler(new CustomerEventHandler())
        .registerHandler(new CustomerResultHandler())
        .build();

    testDriver = new TopologyTestDriver(easySourcing.topology(), properties);

    commandsTopic = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), CustomSerdes.Json(Command.class).serializer());

    commandResultsTopic = testDriver.createOutputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), CustomSerdes.Json(Command.class).deserializer());

    eventsTopic = testDriver.createOutputTopic(CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
        new StringDeserializer(), CustomSerdes.Json(Event.class).deserializer());
  }

  @AfterEach
  void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  @Test
  void test1() {
    Command command = Command.builder()
        .payload(CreateCustomer.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .metadata(Metadata.builder()
            .entry("custom-key", "custom-value")
            .entry(CORRELATION_ID, UUID.randomUUID().toString())
            .entry(ID, "should-be-overwritten-by-command-id")
            .entry(TIMESTAMP, "should-be-overwritten-by-command-timestamp")
            .entry(RESULT, "should-be-overwritten-by-command-result")
            .entry(FAILURE, "should-be-overwritten-by-command-result")
            .build())
        .build();

    commandsTopic.pipeInput(command.getAggregateId(), command);

    // Assert Command Metadata
    assertThat(command.getMetadata().get(ID), is(notNullValue()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(notNullValue()));

    // Assert Command Result
    Command commandResult = commandResultsTopic.readValue();
    assertThat(commandResult, is(notNullValue()));
    assertThat(commandResult.getAggregateId(), is(command.getAggregateId()));
    // Metadata
    assertThat(commandResult.getMetadata(), is(notNullValue()));
    assertThat(commandResult.getMetadata().get("custom-key"), is("custom-value"));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(commandResult.getMetadata().get(ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(RESULT), is("success"));
//    assertThat(commandResult.getMetadata().get(FAILURE), isEmptyOrNullString());
    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));

    // Assert Event
    Event event = eventsTopic.readValue();
    assertThat(event, is(notNullValue()));
    assertThat(event.getAggregateId(), is(command.getAggregateId()));
    // Metadata
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().get("custom-key"), is("custom-value"));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(notNullValue()));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(event.getMetadata().get(ID), is(notNullValue()));
    assertThat(event.getMetadata().get(TIMESTAMP), is(notNullValue()));
//    assertThat(event.getMetadata().get(RESULT), isEmptyOrNullString());
//    assertThat(event.getMetadata().get(FAILURE), isEmptyOrNullString());
    // Payload
    assertThat(event.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(((CustomerCreated) event.getPayload()).getId(), is(((CreateCustomer) command.getPayload()).getId()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(((CreateCustomer) command.getPayload()).getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(((CreateCustomer) command.getPayload()).getCredits()));
    assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(((CreateCustomer) command.getPayload()).getBirthday()));
  }


}
