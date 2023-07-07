package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder
public class BerthVisit {
  /**
   * Always lowercase
   */
  private String id;
  private Berth berth;
  private Instant eta;
  private Instant ata;
  private Instant etd;
  private Instant requestedEtd;
  private Instant atd;
}
