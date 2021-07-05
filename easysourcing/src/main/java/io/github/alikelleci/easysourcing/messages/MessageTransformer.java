package io.github.alikelleci.easysourcing.messages;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class MessageTransformer implements ValueTransformer<JsonNode, Message<?>> {

  private ProcessorContext context;

  private final Class<? extends Message<?>> type;

  public MessageTransformer(Class<? extends Message<?>> type) {
    this.type = type;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Message<?> transform(JsonNode jsonNode) {
    try {
      Message<?> message = JacksonUtils.enhancedObjectMapper().convertValue(jsonNode, type);
      if (message != null) {
        message.getMetadata().injectContext(context);
      }
      return message;
    } catch (Exception e) {
      log.error("Error deserializing JSON", ExceptionUtils.getRootCause(e));
      return null;
    }
  }

  @Override
  public void close() {

  }


}
