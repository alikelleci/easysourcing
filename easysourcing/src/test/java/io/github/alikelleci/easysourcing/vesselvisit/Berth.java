package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Berth {
  private String name;
  private String code;
  private String berthGroupCode;
  private String partyToNotify;
  private Terminal terminal;

  /**
   * Replaced by Terminal object
   */
  @Deprecated
  private String terminalCode;
}
