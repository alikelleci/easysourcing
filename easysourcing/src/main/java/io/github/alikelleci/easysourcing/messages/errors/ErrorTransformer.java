package io.github.alikelleci.easysourcing.messages.errors;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.OperationMode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.Result;
import io.github.alikelleci.easysourcing.messages.Result.Processed;
import io.github.alikelleci.easysourcing.messages.Result.Unprocessed;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

import static io.github.alikelleci.easysourcing.EasySourcingBuilder.OPERATION_MODE;

@Slf4j
public class ErrorTransformer implements Transformer<String, JsonNode, KeyValue<String, Result>> {

  private final MultiValuedMap<Class<?>, ErrorHandler> errorHandlers;
  private ProcessorContext context;
  private KeyValueStore<String, Long> redirects;

  public ErrorTransformer(MultiValuedMap<Class<?>, ErrorHandler> errorHandlers) {
    this.errorHandlers = errorHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
    this.redirects = context.getStateStore("error-redirects");
  }

  @Override
  public KeyValue<String, Result> transform(String key, JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return null;
    }

    Collection<ErrorHandler> handlers = errorHandlers.get(command.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    if (redirects.get(key) != null) {
      if (OPERATION_MODE == OperationMode.NORMAL) {
        log.debug("Redirecting error {} ({})", command.getClass().getSimpleName(), key);
        return KeyValue.pair(key, Unprocessed.builder()
            .payload(command)
            .build());
      }

      if (OPERATION_MODE == OperationMode.RETRY) {
        String error = Optional.ofNullable(context.headers().lastHeader("$error"))
            .map(Header::value)
            .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
            .orElse(null);

        if (StringUtils.isBlank(error)) {
          log.debug("Redirecting error {} ({})", command.getClass().getSimpleName(), key);
          return KeyValue.pair(key, Unprocessed.builder()
              .payload(command)
              .build());
        }
      }
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    try {
      handlers.stream()
          .sorted(Comparator.comparingInt(ErrorHandler::getPriority).reversed())
          .forEach(handler ->
              handler.invoke(command, metadata));
    } catch (Exception e) {
      String message = ExceptionUtils.getRootCauseMessage(e);
      context.headers()
          .remove("$error")
          .add("$error", message.getBytes(StandardCharsets.UTF_8));

      log.error("Error not processed: {}", message);

      redirects.put(key, 1L);
      return KeyValue.pair(key, Unprocessed.builder()
          .payload(command)
          .build());
    }

    redirects.put(key, null);
    return KeyValue.pair(key, Processed.builder()
        .payload(command)
        .build());
  }

  @Override
  public void close() {

  }


}
