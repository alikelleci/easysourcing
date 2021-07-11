package io.github.alikelleci.easysourcing.messages;

import lombok.Builder;
import lombok.Value;


public interface Result {

  Object getPayload();

  @Value
  @Builder
  class Processed implements Result {
    private Object payload;
  }


  @Value
  @Builder
  class Unprocessed implements Result {
    private Object payload;
    private String message;
  }

}
