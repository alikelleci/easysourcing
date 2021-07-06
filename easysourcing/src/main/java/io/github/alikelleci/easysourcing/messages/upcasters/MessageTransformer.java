package io.github.alikelleci.easysourcing.messages.upcasters;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.easysourcing.messages.Message;
import io.github.alikelleci.easysourcing.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

@Slf4j
public class MessageTransformer<A, B> implements ValueTransformer<A, B> {

  private ProcessorContext context;

  private final ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
  private final Class<B> type;

  public MessageTransformer(Class<B> type) {
    this.type = type;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public B transform(A a) {
    try {
      B result = objectMapper.convertValue(a, type);
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
