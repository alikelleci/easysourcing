package io.github.alikelleci.easysourcing.support.upcaster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.easysourcing.support.upcaster.annotations.Upcast;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class UpcastTransformer implements ValueTransformer<JsonNode, JsonNode> {

  private ProcessorContext context;

  private final MultiValuedMap<String, UpcastHandler> upcasters;

  public UpcastTransformer(MultiValuedMap<String, UpcastHandler> upcasters) {
    this.upcasters = upcasters;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public JsonNode transform(JsonNode jsonNode) {
    Collection<UpcastHandler> handlers = upcasters.get(jsonNode.get("payload").get("@class").textValue());
    if (CollectionUtils.isEmpty(handlers)) {
      return jsonNode;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .forEach(handler -> {
          ObjectNode upcastedPayload = (ObjectNode) handler.invoke(jsonNode, context);
          ((ObjectNode) jsonNode.get("payload")).setAll(upcastedPayload);
        });

    return jsonNode;
  }

  @Override
  public void close() {

  }


}
