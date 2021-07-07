package io.github.alikelleci.easysourcing.messages.upcasters;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.upcasters.annotations.Upcast;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;


public class PayloadTransformer implements ValueTransformer<JsonNode, JsonNode> {

  private ProcessorContext context;
  private final MultiValuedMap<String, Upcaster> upcasters;

  public PayloadTransformer(MultiValuedMap<String, Upcaster> upcasters) {
    this.upcasters = upcasters;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public JsonNode transform(JsonNode jsonNode) {
    String className = Optional.ofNullable(jsonNode)
        .map(node -> node.get("@class"))
        .map(JsonNode::textValue)
        .orElse(null);

    if (StringUtils.isBlank(className)) {
      return null;
    }

    Collection<Upcaster> handlers = upcasters.get(className);
    if (CollectionUtils.isEmpty(handlers)) {
      return jsonNode;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);
    AtomicInteger revision = new AtomicInteger(Integer.parseInt(metadata.get("$revision")));

    handlers.stream()
        .sorted(Comparator.comparingInt(handler -> handler.getMethod().getAnnotation(Upcast.class).revision()))
        .forEach(handler ->
            handler.invoke(jsonNode, metadata.add("$revision", String.valueOf(revision.getAndIncrement()))));

    return jsonNode;
  }

  @Override
  public void close() {

  }


}
