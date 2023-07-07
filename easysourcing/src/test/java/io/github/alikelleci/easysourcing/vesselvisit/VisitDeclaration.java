package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class VisitDeclaration {
  private PortVisit portVisit;
  private Voyage arrivalVoyage;
}
