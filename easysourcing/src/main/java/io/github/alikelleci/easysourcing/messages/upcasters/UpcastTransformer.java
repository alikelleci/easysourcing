package io.github.alikelleci.easysourcing.messages.upcasters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.easysourcing.messages.upcasters.annotations.Upcast;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;


public class UpcastTransformer implements ValueTransformer<JsonNode, JsonNode> {

  private ProcessorContext context;

  private final MultiValuedMap<String, Upcaster> upcasters;

  public UpcastTransformer(MultiValuedMap<String, Upcaster> upcasters) {
    this.upcasters = upcasters;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public JsonNode transform(JsonNode jsonNode) {
    Collection<Upcaster> handlers = upcasters.get(jsonNode.get("payload").get("@class").textValue());
    if (CollectionUtils.isEmpty(handlers)) {
      return jsonNode;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .forEach(handler -> {
          int sourceRevision = Optional.ofNullable(jsonNode.get("metadata"))
              .map(metadata -> metadata.get("entries"))
              .map(entries -> entries.get("$revision"))
              .map(JsonNode::intValue)
              .orElse(0);

          int targetRevision = handler.getMethod().getAnnotation(Upcast.class).revision();

          if (sourceRevision == targetRevision) {
            ObjectNode payload = (ObjectNode) handler.invoke(jsonNode, context);
            ((ObjectNode) jsonNode).set("payload", payload);
            ((ObjectNode) jsonNode.get("metadata").get("entries")).put("$revision", sourceRevision + 1);
          }
        });

    return jsonNode;
  }

  @Override
  public void close() {

  }


}
