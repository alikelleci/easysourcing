package io.github.alikelleci.easysourcing.core.support.serialization.json.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.github.alikelleci.easysourcing.core.messaging.Message;
import io.github.alikelleci.easysourcing.core.support.serialization.json.custom.InstantDeserializer;
import io.github.alikelleci.easysourcing.core.support.serialization.json.custom.MetadataDeserializer;
import io.github.alikelleci.easysourcing.core.support.serialization.json.custom.MultiValuedMapDeserializer;
import io.github.alikelleci.easysourcing.core.support.serialization.json.custom.MultiValuedMapSerializer;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import org.apache.commons.collections4.MultiValuedMap;
import tools.jackson.databind.DefaultTyping;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import tools.jackson.databind.jsontype.PolymorphicTypeValidator;
import tools.jackson.databind.module.SimpleModule;

import java.time.Instant;

public class JacksonUtils {

  private static JsonMapper jsonMapper;

  private JacksonUtils() {
  }

  public static JsonMapper enhancedJsonMapper() {
    if (jsonMapper == null) {
      SimpleModule customModule = new SimpleModule()
          .addDeserializer(Metadata.class, new MetadataDeserializer())
          .addDeserializer(Instant.class, new InstantDeserializer())
          .addSerializer(MultiValuedMap.class, new MultiValuedMapSerializer())
          .addDeserializer(MultiValuedMap.class, new MultiValuedMapDeserializer());

      PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
          .allowIfSubType(Object.class)
          .build();

      jsonMapper = JsonMapper.builder()
          .findAndAddModules()
//          .addModule(customModule)
          .polymorphicTypeValidator(ptv)
          .changeDefaultPropertyInclusion(incl -> incl.withValueInclusion(JsonInclude.Include.NON_NULL))
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false)
          .configure(DateTimeFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
          .configure(DateTimeFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
          .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, true)
          .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
          .build();
    }
    return jsonMapper;
  }
}
