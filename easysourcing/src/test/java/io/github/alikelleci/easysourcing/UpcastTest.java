package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerResultHandler;
import io.github.alikelleci.easysourcing.messaging.MessageUpcaster;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;

import java.time.Instant;
import java.util.Properties;

class UpcastTest {


  public static void main(String[] args) throws JsonProcessingException {
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


    MockProcessorContext mockProcessorContext = new MockProcessorContext();
    mockProcessorContext.setRecordTimestamp(Instant.now().toEpochMilli());

    MessageUpcaster messageUpcaster = new MessageUpcaster(easySourcing);
    messageUpcaster.init(mockProcessorContext);


    String json = "{\n" +
        "\t\"@class\": \"some.package.Command\",\n" +
        "\t\"type\": \"CreateCustomer\",\n" +
        "\t\"payload\": {\n" +
        "\t\t\"@class\": \"some.package.Customer\",\n" +
        "\t\t\"name\": \"John Doe\"\n" +
        "\t},\n" +
        "\t\"metadata\": {\n" +
        "\t\t\"entries\": {\n" +
        "\t\t\t\"$id\": \"some-message-id\",\n" +
        "\t\t\t\"$result\": \"failed\",\n" +
        "\t\t\t\"$failure\": \"some-root-cause\",\n" +
        "\t\t\t\"$correlationId\": \"some-correlation-id\",\n" +
        "\t\t\t\"$replyTo\": \"some-reply-to-address\",\n" +
        "\t\t\t\"issuer\": \"Henk\",\n" +
        "\t\t\t\"organisation\": \"MEARSK\"\n" +
        "\t\t}\n" +
        "\t}\n" +
        "}";
    JsonNode root = easySourcing.getObjectMapper().readValue(json, JsonNode.class);


    JsonNode transformed = messageUpcaster.transform("some-key", root);
    System.out.println(easySourcing.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(root));
  }



}
