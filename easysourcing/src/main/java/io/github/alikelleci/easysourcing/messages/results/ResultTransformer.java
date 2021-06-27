package io.github.alikelleci.easysourcing.messages.results;

import io.github.alikelleci.easysourcing.messages.MetadataKeys;
import io.github.alikelleci.easysourcing.messages.commands.Command;
import io.github.alikelleci.easysourcing.messages.results.annotations.HandleError;
import io.github.alikelleci.easysourcing.messages.results.annotations.HandleResult;
import io.github.alikelleci.easysourcing.messages.results.annotations.HandleSuccess;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;


public class ResultTransformer implements ValueTransformer<Command, Void> {

  private ProcessorContext context;

  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers;

  public ResultTransformer(MultiValuedMap<Class<?>, ResultHandler> resultHandlers) {
    this.resultHandlers = resultHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Command command) {
    Collection<ResultHandler> handlers = resultHandlers.get(command.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(ResultHandler::getPriority).reversed())
        .forEach(handler -> {
          boolean handleAll = handler.getMethod().isAnnotationPresent(HandleResult.class);
          boolean handleSuccess = handler.getMethod().isAnnotationPresent(HandleSuccess.class);
          boolean handleFailed = handler.getMethod().isAnnotationPresent(HandleError.class);

          String result = command.getMetadata().get(MetadataKeys.RESULT);
          if (handleAll ||
              (handleSuccess && StringUtils.equals(result, "success")) ||
              (handleFailed && StringUtils.equals(result, "failed"))) {
            handler.invoke(command, context);
          }
        });

    return null;
  }

  @Override
  public void close() {

  }


}
