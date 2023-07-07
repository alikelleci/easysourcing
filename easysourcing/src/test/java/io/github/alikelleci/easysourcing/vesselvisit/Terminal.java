package io.github.alikelleci.easysourcing.vesselvisit;

import lombok.Builder;
import lombok.Value;

@Value
@Builder(toBuilder = true)
public class Terminal {
  private String name;             // quay and terminal is used as synonym, so this is a quay name
  private String code;             // quay code
  private String ownerShortName;
  private String ownerFullName;

  // SVDD
  private Port location;
}
