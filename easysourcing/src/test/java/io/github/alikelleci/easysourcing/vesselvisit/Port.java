package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Port {
  private String name;
  private String unCode;
  private String countryUnCode;
}
