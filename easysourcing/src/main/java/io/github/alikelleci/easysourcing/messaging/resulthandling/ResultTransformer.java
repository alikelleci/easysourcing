package io.github.alikelleci.easysourcing.messaging.resulthandling;

import io.github.alikelleci.easysourcing.EasySourcing;
import io.github.alikelleci.easysourcing.messaging.Metadata;
import io.github.alikelleci.easysourcing.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.messaging.resulthandling.annotations.HandleFailure;
import io.github.alikelleci.easysourcing.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.easysourcing.messaging.resulthandling.annotations.HandleSuccess;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class ResultTransformer implements ValueTransformerWithKey<String, Command, Command> {

  private final EasySourcing easySourcing;

  public ResultTransformer(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(ProcessorContext context) {
  }

  @Override
  public Command transform(String key, Command command) {
    Collection<ResultHandler> resultHandlers = easySourcing.getResultHandlers().get(command.getPayload().getClass());
    if (CollectionUtils.isNotEmpty(resultHandlers)) {
      resultHandlers.stream()
          .sorted(Comparator.comparingInt(ResultHandler::getPriority).reversed())
          .forEach(handler -> {
            boolean handleAll = handler.getMethod().isAnnotationPresent(HandleResult.class);
            boolean handleSuccess = handler.getMethod().isAnnotationPresent(HandleSuccess.class);
            boolean handleFailure = handler.getMethod().isAnnotationPresent(HandleFailure.class);

            String result = command.getMetadata().get(Metadata.RESULT);
            if (handleAll ||
                (handleSuccess && StringUtils.equals(result, "success")) ||
                (handleFailure && StringUtils.equals(result, "failure"))) {
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
