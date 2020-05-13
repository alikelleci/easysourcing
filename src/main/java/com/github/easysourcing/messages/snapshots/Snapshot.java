package com.github.easysourcing.messages.snapshots;

import com.github.easysourcing.messages.Message;
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
public class Snapshot extends Message {

  private String type;

  public String getType() {
    if (getPayload() == null) {
      return null;
    }
    return getPayload().getClass().getSimpleName();
  }

}
