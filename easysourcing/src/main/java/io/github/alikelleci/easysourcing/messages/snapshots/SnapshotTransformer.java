package io.github.alikelleci.easysourcing.messages.snapshots;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.Result;
import io.github.alikelleci.easysourcing.messages.events.EventHandler;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

import static io.github.alikelleci.easysourcing.EasySourcingBuilder.OPERATION_MODE;

@Slf4j
public class SnapshotTransformer implements Transformer<String, JsonNode, KeyValue<String, Result>> {

  private final MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, Long> redirects;

  public SnapshotTransformer(MultiValuedMap<Class<?>, SnapshotHandler> snapshotHandlers) {
    this.snapshotHandlers = snapshotHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.redirects = context.getStateStore("snapshot-redirects");
  }

  @Override
  public KeyValue<String, Result> transform(String key, JsonNode jsonNode) {
    Object snapshot = JsonUtils.toJavaType(jsonNode);
    if (snapshot == null) {
      return null;
    }

    Collection<SnapshotHandler> handlers = snapshotHandlers.get(snapshot.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    if (redirects.get(key) != null) {
      if (OPERATION_MODE == OperationMode.NORMAL) {
        log.debug("Redirecting snapshot {} ({})", snapshot.getClass().getSimpleName(), key);
        return KeyValue.pair(key, Result.Unprocessed.builder()
            .payload(snapshot)
            .build());
      }

      if (OPERATION_MODE == OperationMode.RETRY) {
        String error = Optional.ofNullable(context.headers().lastHeader("$error"))
            .map(Header::value)
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .orElse(null);

        if (StringUtils.isBlank(error)) {
          log.debug("Redirecting snapshot {} ({})", snapshot.getClass().getSimpleName(), key);
          return KeyValue.pair(key, Result.Unprocessed.builder()
              .payload(snapshot)
              .build());
        }
      }
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    try {
      handlers.stream()
          .sorted(Comparator.comparingInt(SnapshotHandler::getPriority).reversed())
          .forEach(handler ->
              handler.invoke(snapshot, metadata));
    } catch (Exception e) {
      String message = ExceptionUtils.getRootCauseMessage(e);
      context.headers()
          .remove("$error")
          .add("$error", message.getBytes(StandardCharsets.UTF_8));

      log.error("Snapshot not processed: {}", message);

      redirects.put(key, 1L);
      return KeyValue.pair(key, Result.Unprocessed.builder()
          .payload(snapshot)
          .build());
    }

    redirects.put(key, null);
    return KeyValue.pair(key, Result.Processed.builder()
        .payload(snapshot)
        .build());
  }

  @Override
  public void close() {

  }


}
