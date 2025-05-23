package io.github.alikelleci.easysourcing.messaging.resulthandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.messaging.resulthandling.annotations.HandleError;
import io.github.alikelleci.easysourcing.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.easysourcing.messaging.resulthandling.annotations.HandleSuccess;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

import static io.github.alikelleci.easysourcing.messaging.Metadata.RESULT;

@Slf4j
public class ResultTransformer implements ValueTransformerWithKey<String, Command, Command> {

  private final EasySourcing easySourcing;
  private ProcessorContext context;

  public ResultTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public Command transform(String key, Command command) {
    Collection<ResultHandler> resultHandlers = easySourcing.getResultHandlers().get(command.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(resultHandlers)) {
      resultHandlers.stream()
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
              log.debug("Handling command result: {} ({})", command.getType(), command.getAggregateId());
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
