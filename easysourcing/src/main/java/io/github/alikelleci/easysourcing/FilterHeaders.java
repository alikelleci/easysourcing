package io.github.alikelleci.easysourcing;

import io.github.alikelleci.easysourcing.messages.Metadata;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;


public class FilterHeaders implements ValueTransformer<Object, Object> {

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

    return object;
  }

  @Override
  public void close() {

  }


}
