package io.github.alikelleci.easysourcing.core.messaging.resulthandling;

import io.github.alikelleci.easysourcing.core.EasySourcing;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.Command;
import io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations.HandleError;
import io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations.HandleSuccess;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.Collection;
import java.util.Comparator;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.RESULT;

@Slf4j
public class ResultProcessor implements FixedKeyProcessor<String, Command, Command> {

  private final EasySourcing easySourcing;
  private FixedKeyProcessorContext<String, Command> context;

  public ResultProcessor(EasySourcing easySourcing) {
    this.easySourcing = easySourcing;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Command> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    Command command = fixedKeyRecord.value();

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

    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {

  }


}
