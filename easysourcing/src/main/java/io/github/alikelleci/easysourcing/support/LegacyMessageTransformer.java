package io.github.alikelleci.easysourcing.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.easysourcing.EasySourcing;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class LegacyMessageTransformer implements ValueTransformerWithKey<String, JsonNode, JsonNode> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;

  public LegacyMessageTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public JsonNode transform(String key, JsonNode root) {
    if (shouldUpcast(root)) {
      // Remove message type info
      ((ObjectNode) root).remove("@class");

      // Extract metadata info
      ObjectNode oldMetadata = ((ObjectNode) root.get("metadata"));
      ObjectNode newMetadata = easySourcing.getObjectMapper().createObjectNode();

      // id & $id
      Optional.ofNullable(oldMetadata.get("entries"))
          .map(node -> node.get("$id"))
          .map(JsonNode::textValue)
          .ifPresent(value -> {
            ((ObjectNode) root).put("id", value);
            newMetadata.put("$id", value);
            ((ObjectNode) oldMetadata.get("entries")).remove("$id");
          });

      // aggregateId & $aggregateId
      ((ObjectNode) root).put("aggregateId", key);
      newMetadata.put("$aggregateId", key);

      // timestamp & $timestamp
      ((ObjectNode) root).put("timestamp", Instant.ofEpochMilli(context.timestamp()).toString());
      newMetadata.put("$timestamp", Instant.ofEpochMilli(context.timestamp()).toString());

      // $result
      Optional.ofNullable(oldMetadata.get("entries"))
          .map(node -> node.get("$result"))
          .map(JsonNode::textValue)
          .ifPresent(value -> {
            newMetadata.put("$result", StringUtils.replace(value, "failed", "failure"));
            ((ObjectNode) oldMetadata.get("entries")).remove("$result");
          });

      // $failure
      Optional.ofNullable(oldMetadata.get("entries"))
          .map(node -> node.get("$failure"))
          .map(JsonNode::textValue)
          .ifPresent(value -> {
            newMetadata.put("$cause", value);
            ((ObjectNode) oldMetadata.get("entries")).remove("$failure");
          });

      // $correlationId
      Optional.ofNullable(oldMetadata.get("entries"))
          .map(node -> node.get("$correlationId"))
          .map(JsonNode::textValue)
          .ifPresent(value -> {
            newMetadata.put("$correlationId", value);
            ((ObjectNode) oldMetadata.get("entries")).remove("$correlationId");
          });

      // $replyTo
      Optional.ofNullable(oldMetadata.get("entries"))
          .map(node -> node.get("$replyTo"))
          .map(JsonNode::textValue)
          .ifPresent(value -> {
            newMetadata.put("$replyTo", value);
            ((ObjectNode) oldMetadata.get("entries")).remove("$replyTo");
          });

      // other metadata info
      Iterator<Map.Entry<String, JsonNode>> it = oldMetadata.get("entries").fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> entry = it.next();
        newMetadata.put(entry.getKey(), entry.getValue());
      }

      // replace old metadata with new one
      ((ObjectNode) root).remove("metadata");
      ((ObjectNode) root).put("metadata", newMetadata);

    }


    return root;

  }

  @Override
  public void close() {

  }

  private boolean shouldUpcast(JsonNode root) {
    JsonNode entries = Optional.ofNullable(root.get("metadata"))
        .map(s -> s.get("entries"))
        .orElse(null);

    return entries != null;
  }


}