package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Vessel {
  private String imoCode;
  private String name;
  private String flagCode;
  private String flagCountryUnCode;
  private String radioCallSign;
}
