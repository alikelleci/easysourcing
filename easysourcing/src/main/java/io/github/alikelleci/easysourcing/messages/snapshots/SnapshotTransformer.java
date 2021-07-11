package io.github.alikelleci.easysourcing.messages.snapshots;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class SnapshotTransformer implements ValueTransformer<JsonNode, Void> {

  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers;
  private ProcessorContext context;

  public SnapshotTransformer(MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers) {
    this.snapshotHandlers = snapshotHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(JsonNode jsonNode) {
    Object snapshot = JsonUtils.toJavaType(jsonNode);
    if (snapshot == null) {
      return null;
    }

    Collection<SnapshotHandler> handlers = snapshotHandlers.get(snapshot.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(SnapshotHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(snapshot, metadata));

    return null;
  }

  @Override
  public void close() {

  }


}
