package io.github.alikelleci.easysourcing.messages;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.nio.charset.StandardCharsets;
import java.util.UUID;


public class AddSnapshotHeaders implements ValueTransformer<Object, Object> {

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

    return object;
  }

  @Override
  public void close() {

  }


}
