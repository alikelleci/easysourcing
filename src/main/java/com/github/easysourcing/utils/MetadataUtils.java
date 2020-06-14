package com.github.easysourcing.utils;

import com.github.easysourcing.messages.Metadata;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

@UtilityClass
public class MetadataUtils {


  public Metadata filterMetadata(Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    Map<String, String> modified = new HashMap<>(metadata.getEntries());
    modified.keySet().removeIf(key ->
        StringUtils.equalsAny(key,"$id", "$result", "$snapshot", "$events", "$failure", "$timestamp"));

    return metadata.toBuilder()
        .clearEntries()
        .entries(modified)
        .build();
  }

}
