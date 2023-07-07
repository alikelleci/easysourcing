package io.github.alikelleci.easysourcing.vesselvisit;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Set;

@TopicInfo("events.com.portbase.cargo.vesselvisit.v3")
public interface VesselVisitEvent {

  @Value
  @Builder
  class VesselVisitRegistered implements VesselVisitEvent {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private PortOfCall portOfCall;
    private Vessel vessel;
    private VisitDeclaration visitDeclaration;
    private Instant etaPortAis;
    private boolean ignoreEtaPortAis;
    private boolean cancelled;
  }

  @Value
  @Builder
  class VesselVisitUpdated implements VesselVisitEvent {
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
  class EtaChanged implements VesselVisitEvent {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id; // crn
    private Instant oldEta;
    private Instant newEta;
  }

  @Value
  @Builder
  class AtdRegistered implements VesselVisitEvent {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id; // crn
    private Instant atd;
  }

  @Value
  @Builder
  class AtaRegistered implements VesselVisitEvent {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id; // crn
    private Instant ata;
  }


  @Value
  @Builder
  class Tracked implements VesselVisitEvent {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
  }

  @Value
  @Builder
  class Untracked implements VesselVisitEvent {
    /**
     * ID is call reference number in lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
  }

  @Value
  @Builder
  class Pinned implements VesselVisitEvent {
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
  class Unpinned implements VesselVisitEvent {
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
  class LabelsUpdated implements VesselVisitEvent {
    /**
     * Always lowercase
     */
    @AggregateId
    private String id;
    private String orgShortName;
    private Set<String> labels;
  }
}
