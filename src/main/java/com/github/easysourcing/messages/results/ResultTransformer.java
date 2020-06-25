package com.github.easysourcing.messages.results;

import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.results.annotations.HandleError;
import com.github.easysourcing.messages.results.annotations.HandleResult;
import com.github.easysourcing.messages.results.annotations.HandleSuccess;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.concurrent.ConcurrentMap;


public class ResultTransformer implements ValueTransformer<Command, Void> {

  private ProcessorContext context;

  private final ConcurrentMap<Class<?>, ResultHandler> resultHandlers;
  private final boolean frequentCommits;

  public ResultTransformer(ConcurrentMap<Class<?>, ResultHandler> resultHandlers, boolean frequentCommits) {
    this.resultHandlers = resultHandlers;
    this.frequentCommits = frequentCommits;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(Command command) {
    ResultHandler resultHandler = resultHandlers.get(command.getPayload().getClass());
    if (resultHandler == null) {
      return null;
    }

    boolean handleAll = resultHandler.getMethod().isAnnotationPresent(HandleResult.class);
    boolean handleSuccess = resultHandler.getMethod().isAnnotationPresent(HandleSuccess.class);
    boolean handleFailed = resultHandler.getMethod().isAnnotationPresent(HandleError.class);

    String result = command.getMetadata().getEntries().get("$result");
    Void v = null;

    if (handleAll ||
        (handleSuccess && StringUtils.equals(result, "success")) ||
        (handleFailed && StringUtils.equals(result, "failed"))) {
      v = resultHandler.invoke(command, context);
    }

    if (frequentCommits) {
      context.commit();
    }
    return v;
  }

  @Override
  public void close() {

  }


}
