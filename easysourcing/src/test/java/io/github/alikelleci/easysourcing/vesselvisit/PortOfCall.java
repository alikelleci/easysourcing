package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PortOfCall {
  private CustomsOffice customsOffice;
  private Port port;
}
