package io.github.alikelleci.easysourcing.vesselvisit;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Set;

@TopicInfo("commands.com.portbase.cargo.vesselvisit.v3")
public interface VesselVisitCommand {

  @Value
  @Builder(toBuilder = true)
  class RegisterVesselVisit implements VesselVisitCommand {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id; // crn
    private PortOfCall portOfCall;
    private Vessel vessel;
    private VisitDeclaration visitDeclaration;
    private Instant etaPortAis;
    private boolean ignoreEtaPortAis;
    private boolean cancelled;
  }

  @Value
  @Builder(toBuilder = true)
  class UpdateVesselVisit implements VesselVisitCommand {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id; // crn
    private PortOfCall portOfCall;
    private Vessel vessel;
    private VisitDeclaration visitDeclaration;
    private Instant etaPortAis;
    private boolean ignoreEtaPortAis;
    private boolean cancelled;
  }

  @Value
  @Builder
  class Track implements VesselVisitCommand {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
  }

  @Value
  @Builder
  class Untrack implements VesselVisitCommand {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
  }

  @Value
  @Builder
  class Pin implements VesselVisitCommand {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
    private String userName;
  }

  @Value
  @Builder
  class Unpin implements VesselVisitCommand {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
    private String userName;
  }

  @Value
  @Builder
  class UpdateLabels implements VesselVisitCommand {
    /**
     * Always lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
    private Set<String> labels;
  }
}
