package io.github.alikelleci.easysourcing.messages.upcasters;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.upcasters.annotations.Upcast;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
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
    String rootType = Optional.ofNullable(jsonNode.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    String payloadType = Optional.ofNullable(jsonNode.get("payload"))
        .map(payload -> payload.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isAnyBlank(rootType, payloadType)) {
      return null;
    }

    Collection<Upcaster> handlers = upcasters.get(payloadType);
    if (CollectionUtils.isEmpty(handlers)) {
      return jsonNode;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .forEach(handler ->
            handler.invoke(jsonNode, context));

    return jsonNode;
  }

  @Override
  public void close() {

  }


}
