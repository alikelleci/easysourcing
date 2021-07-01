package io.github.alikelleci.easysourcing.core.exceptions;

import io.github.alikelleci.easysourcing.core.commands.Command;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class ExceptionTransformer implements ValueTransformer<Command, Void> {

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
  public Void transform(Command command) {
    Collection<ExceptionHandler> handlers = exceptionHandlers.get(command.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(ExceptionHandler::getPriority).reversed())
        .forEach(handler ->
            handler.invoke(command, context));

    return null;
  }

  @Override
  public void close() {

  }


}
