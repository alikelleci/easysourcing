package io.github.alikelleci.easysourcing.messages.commands.transformers;

import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.commands.CommandResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.Optional;


public class FilterReplyTo implements ValueTransformer<CommandResult, CommandResult> {

  private ProcessorContext context;


  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public CommandResult transform(CommandResult object) {
    String replyTo = Optional.ofNullable(context.headers().lastHeader(Metadata.REPLY_TO))
        .map(Header::value)
        .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
        .orElse(null);

    if (StringUtils.isNotBlank(replyTo)) {
      return object;
    }
    return null;
  }

  @Override
  public void close() {

  }


}
