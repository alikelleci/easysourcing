package io.github.alikelleci.easysourcing.messages;

import io.github.alikelleci.easysourcing.common.annotations.Revision;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.core.annotation.AnnotationUtils;

import java.nio.charset.StandardCharsets;
import java.util.Optional;


public class RevisionAdder implements ValueTransformer<Object, Object> {

  private ProcessorContext context;


  @Override
  public void init(ProcessorContext processorContext) {
    this.context = processorContext;
  }

  @Override
  public Object transform(Object object) {
    int revision = Optional.ofNullable(AnnotationUtils.findAnnotation(object.getClass(), Revision.class))
        .map(Revision::value)
        .orElse(1);

    context.headers()
        .remove("$revision")
        .add("$revision", String.valueOf(revision).getBytes(StandardCharsets.UTF_8));

    return object;
  }

  @Override
  public void close() {

  }


}
