package io.github.alikelleci.easysourcing.messages.exceptions;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class ExceptionTransformer implements ValueTransformer<JsonNode, Void> {

  private ProcessorContext context;

  private final MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers;

  public ExceptionTransformer(MultiValuedMap<Class<?>, ExceptionHandler> exceptionHandlers) {
    this.exceptionHandlers = exceptionHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(JsonNode jsonCommand) {
    Object command = JsonUtils.toJavaType(jsonCommand);
    if (command == null) {
      return null;
    }

    Collection<ExceptionHandler> handlers = exceptionHandlers.get(command.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(ExceptionHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(command, metadata));

    return null;
  }

  @Override
  public void close() {

  }


}
