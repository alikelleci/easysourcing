package com.github.easysourcing.example.handlers;

import com.github.easysourcing.example.domain.Customer;
import com.github.easysourcing.example.domain.CustomerCommand.AddCredits;
import com.github.easysourcing.example.domain.CustomerCommand.ChangeFirstName;
import com.github.easysourcing.example.domain.CustomerCommand.ChangeLastName;
import com.github.easysourcing.example.domain.CustomerCommand.CreateCustomer;
import com.github.easysourcing.example.domain.CustomerCommand.DeleteCustomer;
import com.github.easysourcing.example.domain.CustomerCommand.IssueCredits;
import com.github.easysourcing.example.domain.CustomerEvent;
import com.github.easysourcing.example.domain.CustomerEvent.CreditsAdded;
import com.github.easysourcing.example.domain.CustomerEvent.CreditsIssued;
import com.github.easysourcing.example.domain.CustomerEvent.CustomerCreated;
import com.github.easysourcing.example.domain.CustomerEvent.CustomerDeleted;
import com.github.easysourcing.example.domain.CustomerEvent.FirstNameChanged;
import com.github.easysourcing.example.domain.CustomerEvent.LastNameChanged;
import com.github.easysourcing.messaging.Metadata;
import com.github.easysourcing.messaging.commandhandling.annotations.HandleCommand;
import lombok.extern.slf4j.Slf4j;

import javax.validation.ValidationException;


@Slf4j
public class CustomerCommandHandler {

  @HandleCommand
  public CustomerEvent handle(Customer state, CreateCustomer command, Metadata metadata) {
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
  public CustomerEvent handle(Customer state, ChangeFirstName command) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return FirstNameChanged.builder()
        .id(command.getId())
        .firstName(command.getFirstName())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(Customer state, ChangeLastName command) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return LastNameChanged.builder()
        .id(command.getId())
        .lastName(command.getLastName())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(Customer state, AddCredits command) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return CreditsAdded.builder()
        .id(command.getId())
        .amount(command.getAmount())
        .build();
  }

  @HandleCommand
  public CustomerEvent handle(Customer state, IssueCredits command) {
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
  public CustomerEvent handle(Customer state, DeleteCustomer command) {
    if (state == null) {
      throw new ValidationException("Customer does not exists.");
    }

    return CustomerDeleted.builder()
        .id(command.getId())
        .build();
  }
}
