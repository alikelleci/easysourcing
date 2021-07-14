package io.github.alikelleci.easysourcing.messages.events;

import io.github.alikelleci.easysourcing.common.annotations.Revision;
import io.github.alikelleci.easysourcing.messages.Metadata;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.core.annotation.AnnotationUtils;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;


public class AddEventHeaders implements ValueTransformer<Object, Object> {

  private ProcessorContext context;


  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Object transform(Object object) {
    context.headers()
        .remove(Metadata.ID)
        .remove(Metadata.REVISION)
        .remove(Metadata.RESULT)
        .remove(Metadata.CAUSE);

    context.headers()
        .add(Metadata.ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

    int revision = Optional.ofNullable(AnnotationUtils.findAnnotation(object.getClass(), Revision.class))
        .map(Revision::value)
        .orElse(1);

    context.headers()
        .add(Metadata.REVISION, String.valueOf(revision).getBytes(StandardCharsets.UTF_8));

    return object;
  }

  @Override
  public void close() {

  }


}
