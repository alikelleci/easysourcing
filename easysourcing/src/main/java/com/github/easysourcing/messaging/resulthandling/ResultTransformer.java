package com.github.easysourcing.messaging.resulthandling;

import com.github.easysourcing.EasySourcing;
import com.github.easysourcing.messaging.commandhandling.Command;
import com.github.easysourcing.messaging.resulthandling.annotations.HandleError;
import com.github.easysourcing.messaging.resulthandling.annotations.HandleResult;
import com.github.easysourcing.messaging.resulthandling.annotations.HandleSuccess;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

import static com.github.easysourcing.messaging.Metadata.RESULT;

@Slf4j
public class ResultTransformer implements ValueTransformerWithKey<String, Command, Command> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;

  public ResultTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Command transform(String key, Command command) {
    Collection<ResultHandler> handlers = easySourcing.getResultHandlers().get(command.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(handlers)) {
      handlers.stream()
          .sorted(Comparator.comparingInt(ResultHandler::getPriority).reversed())
          .peek(handler -> handler.setContext(context))
          .forEach(handler -> {
            boolean handleAll = handler.getMethod().isAnnotationPresent(HandleResult.class);
            boolean handleSuccess = handler.getMethod().isAnnotationPresent(HandleSuccess.class);
            boolean handleFailure = handler.getMethod().isAnnotationPresent(HandleError.class);

            String result = command.getMetadata().get(RESULT);
            if (handleAll ||
                (handleSuccess && StringUtils.equals(result, "success")) ||
                (handleFailure && StringUtils.equals(result, "failed"))) {
              handler.apply(command);
            }
          });
    }

    return command;
  }

  @Override
  public void close() {

  }


}
