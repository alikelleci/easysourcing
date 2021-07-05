package io.github.alikelleci.easysourcing.messages;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class MessageTransformer<T> implements ValueTransformer<JsonNode, T> {

  private ProcessorContext context;

  private final Class<T> type;

  public MessageTransformer(Class<T> type) {
    this.type = type;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public T transform(JsonNode jsonNode) {
    try {
      T message = JacksonUtils.enhancedObjectMapper().convertValue(jsonNode, type);
      if (message instanceof Message) {
        ((Message<?>) message).getMetadata().injectContext(context);
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
