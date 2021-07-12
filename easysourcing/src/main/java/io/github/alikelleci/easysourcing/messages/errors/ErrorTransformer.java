package io.github.alikelleci.easysourcing.messages.errors;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class ErrorTransformer implements ValueTransformerWithKey<String, JsonNode, Void> {

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
  public Void transform(String key, JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return null;
    }

    Collection<ErrorHandler> handlers = errorHandlers.get(command.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(ErrorHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(command, metadata));

    return null;
  }

  @Override
  public void close() {

  }


}
