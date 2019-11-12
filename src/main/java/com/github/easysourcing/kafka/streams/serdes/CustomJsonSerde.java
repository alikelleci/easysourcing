package com.github.easysourcing.kafka.streams.serdes;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonSerde;


public class CustomJsonSerde<T> extends JsonSerde<T> {

  public CustomJsonSerde(Class<? super T> targetType) {
    super(targetType, JacksonUtils.enhancedObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false));
  }
}
