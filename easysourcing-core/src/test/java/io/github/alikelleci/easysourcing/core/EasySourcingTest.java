package io.github.alikelleci.easysourcing.core;

import io.github.alikelleci.easysourcing.core.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.core.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.easysourcing.core.example.handlers.CustomerEventHandler;
import io.github.alikelleci.easysourcing.core.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.easysourcing.core.example.handlers.CustomerResultHandler;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.Event;
import io.github.alikelleci.easysourcing.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.easysourcing.core.support.serialization.json.JsonSerializer;
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

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.easysourcing.core.messaging.Metadata.FAILURE;
import static io.github.alikelleci.easysourcing.core.messaging.Metadata.ID;
import static io.github.alikelleci.easysourcing.core.messaging.Metadata.RESULT;
import static io.github.alikelleci.easysourcing.core.messaging.Metadata.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
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
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "example-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");

    EasySourcing easySourcing = EasySourcing.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .registerHandler(new CustomerEventHandler())
        .registerHandler(new CustomerResultHandler())
        .build();

    testDriver = new TopologyTestDriver(easySourcing.topology(), easySourcing.getStreamsConfig());

    commandsTopic = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), new JsonSerializer<>());

    commandResultsTopic = testDriver.createOutputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), new JsonDeserializer<>(Command.class));

    eventsTopic = testDriver.createOutputTopic(CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
        new StringDeserializer(), new JsonDeserializer<>(Event.class));
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
            .add("custom-key", "custom-value")
            .add(CORRELATION_ID, UUID.randomUUID().toString())
            .add(ID, "should-be-overwritten-by-command-id")
            .add(TIMESTAMP, "should-be-overwritten-by-command-timestamp")
            .add(RESULT, "should-be-overwritten-by-command-result")
            .add(FAILURE, "should-be-overwritten-by-command-result")
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
    assertThat(commandResult.getMetadata().get(FAILURE), emptyOrNullString());
    // Payload
    // assertThat(commandResult.getPayload(), is(command.getPayload())); // TODO: fix date

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
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(FAILURE), emptyOrNullString());
    // Payload
    assertThat(event.getPayload(), instanceOf(CustomerCreated.class));
    assertThat(((CustomerCreated) event.getPayload()).getId(), is(((CreateCustomer) command.getPayload()).getId()));
    assertThat(((CustomerCreated) event.getPayload()).getLastName(), is(((CreateCustomer) command.getPayload()).getLastName()));
    assertThat(((CustomerCreated) event.getPayload()).getCredits(), is(((CreateCustomer) command.getPayload()).getCredits()));
    // assertThat(((CustomerCreated) event.getPayload()).getBirthday(), is(((CreateCustomer) command.getPayload()).getBirthday())); // TODO: fix date
  }


}
