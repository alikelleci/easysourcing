package io.github.alikelleci.easysourcing.messages;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class MessageTransformer<S, T> implements ValueTransformer<S, T> {

  private ProcessorContext context;

  private final ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
  private final Class<T> type;

  public MessageTransformer(Class<T> type) {
    this.type = type;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public T transform(S s) {
    try {
      T result = objectMapper.convertValue(s, type);
      if (result instanceof Message) {
        ((Message<?>) result).getMetadata().injectContext(context);
      }
      return result;
    } catch (Exception e) {
      log.error("Error transforming message", ExceptionUtils.getRootCause(e));
      return null;
    }
  }

  @Override
  public void close() {

  }


}
