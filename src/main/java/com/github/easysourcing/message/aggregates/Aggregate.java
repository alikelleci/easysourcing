package com.github.easysourcing.message.aggregates;

import com.github.easysourcing.message.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class Aggregate extends Message {

  private String type;

  public String getType() {
    if (getPayload() == null) {
      return null;
    }
    return getPayload().getClass().getSimpleName();
  }

}
