package io.github.alikelleci.easysourcing.messages.commands.transformers;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Failure;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult.Success;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class AddResultHeaders implements ValueTransformer<CommandResult, CommandResult> {

  private ProcessorContext context;


  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public CommandResult transform(CommandResult object) {
    context.headers()
        .remove(Metadata.ID)
        .remove(Metadata.REVISION)
        .remove(Metadata.RESULT)
        .remove(Metadata.CAUSE);

    context.headers()
        .add(Metadata.ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

    if (object instanceof Success) {
      context.headers()
          .add(Metadata.RESULT, "success".getBytes(StandardCharsets.UTF_8));
    } else if (object instanceof Failure) {
      Failure failure = (Failure) object;
      context.headers()
          .add(Metadata.RESULT, "failure".getBytes(StandardCharsets.UTF_8))
          .add(Metadata.CAUSE, failure.getCause().getBytes(StandardCharsets.UTF_8));
    }

    return object;
  }

  @Override
  public void close() {

  }


}
