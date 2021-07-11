package io.github.alikelleci.easysourcing.messages.errors;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
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
public class ErrorTransformer implements Transformer<String, JsonNode, KeyValue<String, Void>> {

  private final MultiValuedMap<Class<?>, ErrorHandler> errorHandlers;
  private ProcessorContext context;

  public ErrorTransformer(MultiValuedMap<Class<?>, ErrorHandler> errorHandlers) {
    this.errorHandlers = errorHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public KeyValue<String, Void> transform(String key, JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return KeyValue.pair(key, null);
    }

    Collection<ErrorHandler> handlers = errorHandlers.get(command.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return KeyValue.pair(key, null);
    }

    log.debug("Handling error: {} ({})", command.getClass().getSimpleName(), key);

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(ErrorHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(command, metadata));

    return KeyValue.pair(key, null);
  }

  @Override
  public void close() {

  }


}
