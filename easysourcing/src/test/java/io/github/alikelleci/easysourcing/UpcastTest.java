package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.example.handlers.CustomerCommandHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerEventSourcingHandler;
import io.github.alikelleci.easysourcing.example.handlers.CustomerResultHandler;
import io.github.alikelleci.easysourcing.support.LegacyMessageTransformer;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.MockProcessorContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Properties;

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

    LegacyMessageTransformer legacyMessageTransformer = new LegacyMessageTransformer(easySourcing);
    legacyMessageTransformer.init(mockProcessorContext);

    JsonNode json = objectMapper.readValue(UpcastTest.class.getClassLoader().getResourceAsStream("customer.json"), JsonNode.class);

    System.out.println("Original result:");
    System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

    JsonNode transformed = legacyMessageTransformer.transform("some-aggregate-id", json);

    System.out.println("Transformed result:");
    System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(transformed));

    System.out.println("Java class:");
    Command command = objectMapper.convertValue(transformed, Command.class);
    System.out.println(command);

    System.out.println("Java to JSON:");
    String commandJson = objectMapper.writeValueAsString(command);
    System.out.println(commandJson);



  }



}
