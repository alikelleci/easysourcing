package io.github.alikelleci.easysourcing.vesselvisit;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.beans.Transient;
import java.time.Instant;

@Value
@Builder(toBuilder = true)
@TopicInfo("snapshots.com.portbase.cargo.vesselvisit.v3")
public class VesselVisit {
  @AggregateId
  private String id;
  private PortOfCall portOfCall;
  private Vessel vessel;
  private VisitDeclaration visitDeclaration;
  private Instant etaPortAis;
  private boolean ignoreEtaPortAis;
  private boolean cancelled;
  private Instant dateRegistered;
  
  @Transient
  public String getCrn() {
    return StringUtils.upperCase(id);
  }
}
