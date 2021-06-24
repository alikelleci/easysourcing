package io.github.easysourcing.messages.events;

import io.github.easysourcing.messages.Message;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.Optional;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString(callSuper = true)
@SuperBuilder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
public class Event extends Message {

  private String type;

  public String getType() {
    return Optional.ofNullable(getPayload())
        .map(Object::getClass)
        .map(Class::getSimpleName)
        .orElse(null);
  }

}
