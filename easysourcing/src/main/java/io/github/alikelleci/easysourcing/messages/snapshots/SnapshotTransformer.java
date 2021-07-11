package io.github.alikelleci.easysourcing.messages.snapshots;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class SnapshotTransformer implements Transformer<String, JsonNode, KeyValue<String, Void>> {

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
  public KeyValue<String, Void> transform(String key, JsonNode jsonNode) {
    Object snapshot = JsonUtils.toJavaType(jsonNode);
    if (snapshot == null) {
      return KeyValue.pair(key, null);
    }

    Collection<SnapshotHandler> handlers = snapshotHandlers.get(snapshot.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return KeyValue.pair(key, null);
    }

    log.debug("Handling snapshot: {} ({})", snapshot.getClass().getSimpleName(), key);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(SnapshotHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(snapshot, metadata));

    return KeyValue.pair(key, null);
  }

  @Override
  public void close() {

  }


}
