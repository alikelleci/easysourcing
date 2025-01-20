package io.github.alikelleci.easysourcing.example.domain;

import io.github.alikelleci.easysourcing.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.common.annotations.TopicInfo;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@TopicInfo("commands.customer")
public interface CustomerCommand {

  @Value
  @Builder
  class CreateCustomer implements CustomerCommand {
    @AggregateId
    private String id;
    @NotBlank
    private String firstName;
    @NotBlank
    private String lastName;
    @Max(100)
    private int credits;
    private Instant birthday;
  }

  @Value
  @Builder
  class ChangeFirstName implements CustomerCommand {
    @AggregateId
    private String id;
    @NotBlank
    private String firstName;
  }

  @Value
  @Builder
  class ChangeLastName implements CustomerCommand {
    @AggregateId
    private String id;
    @NotBlank
    private String lastName;
  }

  @Value
  @Builder
  class AddCredits implements CustomerCommand {
    @AggregateId
    private String id;
    private int amount;
  }

  @Value
  @Builder
  class IssueCredits implements CustomerCommand {
    @AggregateId
    private String id;
    private int amount;
  }

  @Value
  @Builder
  class DeleteCustomer implements CustomerCommand {
    @AggregateId
    private String id;
  }
}
