package io.github.alikelleci.easysourcing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.util.JacksonUtils;

import java.io.IOException;
import java.net.URISyntaxException;

class UpcastTest {


  public static void main(String[] args) throws IOException, URISyntaxException {
    ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

    JsonNode json = objectMapper.readValue(UpcastTest.class.getClassLoader().getResourceAsStream("create-customer.json"), JsonNode.class);

    System.out.println("Original:");
    System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

    System.out.println("Java class:");
    Command command = objectMapper.convertValue(json, Command.class);
    System.out.println(command);

    System.out.println("Java to JSON:");
    String commandJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(command);
    System.out.println(commandJson);



  }



}
