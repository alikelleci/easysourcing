package io.github.alikelleci.easysourcing.core.example.handlers;

import io.github.alikelleci.easysourcing.core.example.domain.Customer;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.annotations.ApplyEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerEventSourcingHandler {

  @ApplyEvent
  public Customer handle(Customer state, CustomerCreated event, Metadata metadata) {
    return Customer.builder()
        .id(event.getId())
        .firstName(event.getFirstName())
        .lastName(event.getLastName())
        .credits(event.getCredits())
        .birthday(event.getBirthday())
        .dateCreated(metadata.getTimestamp())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, FirstNameChanged event) {
    return state.toBuilder()
        .firstName(event.getFirstName())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, LastNameChanged event) {
    return state.toBuilder()
        .lastName(event.getLastName())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, CreditsAdded event) {
    return state.toBuilder()
        .credits(state.getCredits() + event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, CreditsIssued event) {
    return state.toBuilder()
        .credits(state.getCredits() - event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(Customer state, CustomerDeleted event) {
    return null;
  }
}
