package com.github.easysourcing.example.domain;

import com.github.easysourcing.common.annotations.AggregateId;
import com.github.easysourcing.common.annotations.TopicInfo;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
@TopicInfo("snapshothandling.customer")
public class Customer {
  @AggregateId
  private String id;
  private String firstName;
  private String lastName;
  private int credits;
  private Instant birthday;
  private Instant dateCreated;
}
