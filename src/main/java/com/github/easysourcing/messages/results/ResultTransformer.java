package com.github.easysourcing.messages.results;

import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.results.annotations.HandleError;
import com.github.easysourcing.messages.results.annotations.HandleResult;
import com.github.easysourcing.messages.results.annotations.HandleSuccess;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;


public class ResultTransformer implements ValueTransformer<Command, List<Command>> {

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
  public List<Command> transform(Command command) {
    ResultHandler resultHandler = resultHandlers.get(command.getPayload().getClass());
    if (resultHandler == null) {
      return null;
    }

    boolean handleAll = resultHandler.getMethod().isAnnotationPresent(HandleResult.class);
    boolean handleSuccess = resultHandler.getMethod().isAnnotationPresent(HandleSuccess.class);
    boolean handleFailed = resultHandler.getMethod().isAnnotationPresent(HandleError.class);

    String result = command.getMetadata().getEntries().get("$result");
    List<Command> commands = new ArrayList<>();

    if (handleAll) {
      commands = resultHandler.invoke(command, context);
    } else if (handleSuccess && result.equals("success")) {
      commands = resultHandler.invoke(command, context);
    } else if (handleFailed && result.equals("failed")) {
      commands = resultHandler.invoke(command, context);
    }

    if (frequentCommits) {
      context.commit();
    }
    return commands;
  }

  @Override
  public void close() {

  }


}
