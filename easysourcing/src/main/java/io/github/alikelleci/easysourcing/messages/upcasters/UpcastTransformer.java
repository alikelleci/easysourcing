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
          int sourceRevision = jsonNode.get("metadata").get("entries").get("$revision").intValue();
          int targetRevision = handler.getMethod().getAnnotation(Upcast.class).revision();

          if (sourceRevision == targetRevision) {
            ObjectNode upcastedPayload = (ObjectNode) handler.invoke(jsonNode, context);
            ((ObjectNode) jsonNode.get("payload")).setAll(upcastedPayload);
            ((ObjectNode) jsonNode.get("metadata").get("entries")).put("$revision", sourceRevision + 1);
          }
        });

    return jsonNode;
  }

  @Override
  public void close() {

  }


}
