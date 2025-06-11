package io.github.alikelleci.easysourcing.core.example.handlers;

import io.github.alikelleci.easysourcing.core.common.annotations.MessageId;
import io.github.alikelleci.easysourcing.core.common.annotations.MetadataValue;
import io.github.alikelleci.easysourcing.core.common.annotations.Timestamp;
import io.github.alikelleci.easysourcing.core.example.domain.Customer;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.AddCredits;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.ChangeFirstName;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.ChangeLastName;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.CreateCustomer;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.DeleteCustomer;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerCommand.IssueCredits;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.commandhandling.annotations.HandleCommand;
import jakarta.validation.ValidationException;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.CORRELATION_ID;


@Slf4j
public class CustomerCommandHandler {

  @HandleCommand
  public CustomerEvent handle(CreateCustomer command,
                              Customer state,
                              Metadata metadata,
                              @Timestamp Instant timestamp,
                              @MessageId String messageId,
                              @MetadataValue(CORRELATION_ID) String correlationId) {
    if (state != null) {
      throw new ValidationException("Customer already exists.");
    }

    return CustomerCreated.builder()
        .id(command.getId())
        .firstName(command.getFirstName())
        .lastName(command.getLastName())
        .credits(command.getCredits())
        .birthday(command.getBirthday())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(ChangeFirstName command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return FirstNameChanged.builder()
        .id(command.getId())
        .firstName(command.getFirstName())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(ChangeLastName command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return LastNameChanged.builder()
        .id(command.getId())
        .lastName(command.getLastName())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(AddCredits command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return CreditsAdded.builder()
        .id(command.getId())
        .amount(command.getAmount())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(IssueCredits command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    if (state.getCredits() < command.getAmount()) {
      throw new ValidationException("Credits not issued: not enough credits available.");
    }

    return CreditsIssued.builder()
        .id(command.getId())
        .amount(command.getAmount())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(DeleteCustomer command, Customer state) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return CustomerDeleted.builder()
        .id(command.getId())
        .build();
  }
}
