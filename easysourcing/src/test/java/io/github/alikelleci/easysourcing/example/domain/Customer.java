package io.github.alikelleci.easysourcing.example.domain;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
public class Customer {
  @AggregateId
  private String id;
  private String firstName;
  private String lastName;
  private int credits;
  private Instant birthday;
  private Instant dateCreated;
}
