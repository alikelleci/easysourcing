package com.github.easysourcing.message.aggregates;

import com.github.easysourcing.message.Message;
import com.github.easysourcing.message.Metadata;
import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Aggregate<T> implements Message<T> {

  private T payload;
  private Metadata metadata;

}
