package io.github.alikelleci.easysourcing.messaging;

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
public class MessageUpcaster implements ValueTransformerWithKey<String, JsonNode, JsonNode> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;

  public MessageUpcaster(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public JsonNode transform(String key, JsonNode root) {

    String messageType = Optional.ofNullable(root.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);


    if (StringUtils.isNotBlank(messageType)) {
      // Remove message type info
      ((ObjectNode) root).remove("@class");

      // Extract metadata info
      ObjectNode oldMetadata = ((ObjectNode) root.get("metadata"));
      ObjectNode newMetadata = easySourcing.getObjectMapper().createObjectNode();

      // $id
      String id = oldMetadata.get("entries").get("$id").textValue();
      ((ObjectNode) root).put("id", id);
      newMetadata.put("$id", id);
      ((ObjectNode) oldMetadata.get("entries")).remove("$id");

      // $timestamp
      ((ObjectNode) root).put("timestamp", Instant.ofEpochMilli(context.timestamp()).toString());
      newMetadata.put("$timestamp", Instant.ofEpochMilli(context.timestamp()).toString());

      // $result
      String result = oldMetadata.get("entries").get("$result").textValue();
      newMetadata.put("$result", StringUtils.replace(result, "failed", "failure"));
      ((ObjectNode) oldMetadata.get("entries")).remove("$result");

      // $failure
      String failure = oldMetadata.get("entries").get("$failure").textValue();
      newMetadata.put("$cause", failure);
      ((ObjectNode) oldMetadata.get("entries")).remove("$failure");

      // $correlationId
      String correlationId = oldMetadata.get("entries").get("$correlationId").textValue();
      newMetadata.put("$correlationId", correlationId);
      ((ObjectNode) oldMetadata.get("entries")).remove("$correlationId");

      // $replyTo
      String replyTo = oldMetadata.get("entries").get("$replyTo").textValue();
      newMetadata.put("$replyTo", replyTo);
      ((ObjectNode) oldMetadata.get("entries")).remove("$replyTo");

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


}
