package io.github.alikelleci.easysourcing.messages.results;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.messages.results.annotations.HandleFailure;
import io.github.alikelleci.easysourcing.messages.results.annotations.HandleResult;
import io.github.alikelleci.easysourcing.messages.results.annotations.HandleSuccess;
import io.github.alikelleci.easysourcing.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

@Slf4j
public class ResultTransformer implements ValueTransformerWithKey<String, JsonNode, Void> {

  private final MultiValuedMap<Class<?>, ResultHandler> resultHandlers;
  private ProcessorContext context;

  public ResultTransformer(MultiValuedMap<Class<?>, ResultHandler> resultHandlers) {
    this.resultHandlers = resultHandlers;
  }

  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Void transform(String key, JsonNode jsonNode) {
    Object command = JsonUtils.toJavaType(jsonNode);
    if (command == null) {
      return null;
    }

    Collection<ResultHandler> handlers = resultHandlers.get(command.getClass());
    if (CollectionUtils.isEmpty(handlers)) {
      return null;
    }

    Metadata metadata = Metadata.builder().build().injectContext(context);

    handlers.stream()
        .sorted(Comparator.comparingInt(ResultHandler::getPriority).reversed())
        .forEach(handler -> {
          boolean handleAll = handler.getMethod().isAnnotationPresent(HandleResult.class);
          boolean handleSuccess = handler.getMethod().isAnnotationPresent(HandleSuccess.class);
          boolean handleFailure = handler.getMethod().isAnnotationPresent(HandleFailure.class);

          String result = Optional.ofNullable(context.headers().lastHeader(Metadata.RESULT))
              .map(Header::value)
              .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
              .orElse(null);

          if (handleAll ||
              (handleSuccess && StringUtils.equals(result, "successful")) ||
              (handleFailure && StringUtils.equals(result, "failed"))) {
            handler.invoke(command, metadata);
          }
        });

    return null;
  }

  @Override
  public void close() {

  }


}
