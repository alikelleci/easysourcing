package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.example.domain.CustomerCommand;
import io.github.alikelleci.easysourcing.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerResultHandler;
import io.github.alikelleci.easysourcing.messaging.MessageUpcaster;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static io.github.alikelleci.easysourcing.messaging.Metadata.CAUSE;
import static io.github.alikelleci.easysourcing.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.easysourcing.messaging.Metadata.ID;
import static io.github.alikelleci.easysourcing.messaging.Metadata.RESULT;
import static io.github.alikelleci.easysourcing.messaging.Metadata.TIMESTAMP;

class UpcastTest {


  public static void main(String[] args) throws IOException, URISyntaxException {
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

    ObjectMapper objectMapper = easySourcing.getObjectMapper();

    MockProcessorContext mockProcessorContext = new MockProcessorContext();
    mockProcessorContext.setRecordTimestamp(1684189825000L);

    MessageUpcaster messageUpcaster = new MessageUpcaster(easySourcing);
    messageUpcaster.init(mockProcessorContext);

    JsonNode json = objectMapper.readValue(Paths.get("C:\\Projects\\private\\easysourcing\\easysourcing\\src\\test\\java\\io\\github\\alikelleci\\easysourcing\\customer.json").toFile(), JsonNode.class);
    JsonNode transformed = messageUpcaster.transform("some-key", json);

    System.out.println("Original result:");
    System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

    System.out.println("Transformed result:");
    System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(transformed));

    System.out.println("Java class:");
    System.out.println(objectMapper.convertValue(transformed, Command.class));


    System.out.println("aaaaaaaaaaaaaaa");
    System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(Command.builder()
        .payload(CustomerCommand.CreateCustomer.builder()
            .id("customer-123")
            .firstName("Peter")
            .lastName("Bruin")
            .credits(100)
            .birthday(Instant.now())
            .build())
        .build()));

  }



}