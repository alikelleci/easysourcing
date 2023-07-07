package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.List;

@Value
@Builder(toBuilder = true)
public class PortVisit {
  private Instant etaPort;
  private List<BerthVisit> berthVisits;
  private Instant etdPort;
  private Instant ataPort;
  private Instant atdPort;
}
