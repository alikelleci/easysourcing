package io.github.alikelleci.easysourcing.core.common;

import io.github.alikelleci.easysourcing.core.common.annotations.MessageId;
import io.github.alikelleci.easysourcing.core.common.annotations.MetadataValue;
import io.github.alikelleci.easysourcing.core.common.annotations.Timestamp;
import io.github.alikelleci.easysourcing.core.messaging.Message;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;

import java.lang.reflect.Parameter;

public interface CommonParameterResolver {

  default Object resolve(Parameter parameter, Message message, FixedKeyProcessorContext<?, ?> context) {
    if (parameter.getType().isAssignableFrom(Metadata.class)) {
      return message.getMetadata().inject(context);
    } else if (parameter.isAnnotationPresent(Timestamp.class)) {
      return message.getMetadata().inject(context).getTimestamp();
    } else if (parameter.isAnnotationPresent(MessageId.class)) {
      return message.getMetadata().inject(context).getMessageId();
    } else if (parameter.isAnnotationPresent(MetadataValue.class)) {
      MetadataValue annotation = parameter.getAnnotation(MetadataValue.class);
      String key = annotation.value();
      return key.isEmpty() ? message.getMetadata().inject(context) : message.getMetadata().inject(context).get(key);
    } else {
      throw new IllegalArgumentException("Unsupported parameter: " + parameter);
    }
  }
}
