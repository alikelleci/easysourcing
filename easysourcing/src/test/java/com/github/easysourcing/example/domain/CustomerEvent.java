package com.github.easysourcing.example.domain;

import com.github.easysourcing.messages.annotations.AggregateId;
import com.github.easysourcing.messages.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@TopicInfo("events.customer")
public interface CustomerEvent {

  @Value
  @Builder
  class CustomerCreated implements CustomerEvent {
    @AggregateId
    private String id;
    private String firstName;
    private String lastName;
    private int credits;
    private Instant birthday;
  }

  @Value
  @Builder
  class FirstNameChanged implements CustomerEvent {
    @AggregateId
    private String id;
    private String firstName;
  }

  @Value
  @Builder
  class LastNameChanged implements CustomerEvent {
    @AggregateId
    private String id;
    private String lastName;
  }

  @Value
  @Builder
  class CreditsAdded implements CustomerEvent {
    @AggregateId
    private String id;
    private int amount;
  }

  @Value
  @Builder
  class CreditsIssued implements CustomerEvent {
    @AggregateId
    private String id;
    private int amount;
  }

  @Value
  @Builder
  class CustomerDeleted implements CustomerEvent {
    @AggregateId
    private String id;
  }
}
