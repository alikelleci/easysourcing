package io.github.alikelleci.easysourcing.core;

import io.github.alikelleci.easysourcing.core.common.annotations.TopicInfo;
import io.github.alikelleci.easysourcing.core.example.customer.core.CustomerCommandHandler;
import io.github.alikelleci.easysourcing.core.example.customer.core.CustomerEventSourcingHandler;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.CreateCustomer;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent;
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
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
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
import static org.assertj.core.api.Assertions.assertThat;

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
//        .registerHandler(new CustomerEventHandler())
//        .registerHandler(new CustomerResultHandler())
        .build();

    testDriver = new TopologyTestDriver(easySourcing.topology());

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

    // Assert Command Result
    Command commandResult = commandResultsTopic.readValue();
    assertThat(commandResult)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("metadata", "payload.birthday")
            .build())
        .isEqualTo(command);

    assertThat(commandResult.getMetadata().get(RESULT)).isEqualTo("success");
    assertThat(commandResult.getMetadata().get(FAILURE)).isBlank();

    // Assert Event
    Event event = eventsTopic.readValue();
    assertThat(event)
        .usingRecursiveComparison(RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("type", "metadata", "payload.birthday")
            .build())
        .isEqualTo(command);
  }

}
