package com.github.easysourcing;

import com.github.easysourcing.example.domain.CustomerCommand;
import com.github.easysourcing.example.domain.CustomerCommand.CreateCustomer;
import com.github.easysourcing.example.domain.CustomerEvent;
import com.github.easysourcing.example.domain.CustomerEvent.CustomerCreated;
import com.github.easysourcing.example.handlers.CustomerCommandHandler;
import com.github.easysourcing.example.handlers.CustomerEventHandler;
import com.github.easysourcing.example.handlers.CustomerEventSourcingHandler;
import com.github.easysourcing.example.handlers.CustomerResultHandler;
import com.github.easysourcing.messages.Metadata;
import com.github.easysourcing.messages.annotations.TopicInfo;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.events.Event;
import com.github.easysourcing.support.serializer.CustomSerdes;
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

import static com.github.easysourcing.messages.Metadata.CORRELATION_ID;
import static com.github.easysourcing.messages.Metadata.FAILURE;
import static com.github.easysourcing.messages.Metadata.ID;
import static com.github.easysourcing.messages.Metadata.RESULT;
import static com.github.easysourcing.messages.Metadata.TIMESTAMP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.notNullValue;

class EasySourcingTest {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, Command> commandResults;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
    properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

    EasySourcing easySourcing = new EasySourcingBuilder(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
        .registerHandler(new CustomerEventHandler())
        .registerHandler(new CustomerResultHandler())
        .build();

    testDriver = new TopologyTestDriver(easySourcing.buildTopology(), properties);

    commands = testDriver.createInputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), CustomSerdes.Json(Command.class).serializer());

    commandResults = testDriver.createOutputTopic(CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), CustomSerdes.Json(Command.class).deserializer());

    events = testDriver.createOutputTopic(CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
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

    commands.pipeInput(command.getAggregateId(), command);

    // Assert Command Metadata
    assertThat(command.getMetadata().get(ID), is(notNullValue()));
    assertThat(command.getMetadata().get(TIMESTAMP), is(notNullValue()));

    // Assert Command Result
    Command commandResult = commandResults.readValue();
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
    Event event = events.readValue();
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
