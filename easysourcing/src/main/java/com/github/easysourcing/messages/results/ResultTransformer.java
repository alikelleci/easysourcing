package com.github.easysourcing.messages.results;

import com.github.easysourcing.constants.Handlers;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.results.annotations.HandleError;
import com.github.easysourcing.messages.results.annotations.HandleResult;
import com.github.easysourcing.messages.results.annotations.HandleSuccess;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

import static com.github.easysourcing.messages.Metadata.RESULT;


public class ResultTransformer implements ValueTransformer<Command, Void> {

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Command command) {
    Collection<ResultHandler> handlers = Handlers.RESULT_HANDLERS.get(command.getPayload().getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    handlers.stream()
        .sorted(Comparator.comparingInt(ResultHandler::getPriority).reversed())
        .forEach(handler -> {
          boolean handleAll = handler.getMethod().isAnnotationPresent(HandleResult.class);
          boolean handleSuccess = handler.getMethod().isAnnotationPresent(HandleSuccess.class);
          boolean handleFailed = handler.getMethod().isAnnotationPresent(HandleError.class);

          String result = command.getMetadata().get(RESULT);
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
