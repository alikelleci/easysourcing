package io.github.alikelleci.easysourcing.core.example.customer.core;

import io.github.alikelleci.easysourcing.core.common.annotations.MessageId;
import io.github.alikelleci.easysourcing.core.common.annotations.MetadataValue;
import io.github.alikelleci.easysourcing.core.common.annotations.Timestamp;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CreditsAdded;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CreditsIssued;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.LastNameChanged;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.annotations.ApplyEvent;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.CORRELATION_ID;

@Slf4j
public class CustomerEventSourcingHandler {

  @ApplyEvent
  public Customer handle(CustomerCreated event,
                         Customer state,
                         Metadata metadata,
                         @Timestamp Instant timestamp,
                         @MessageId String messageId,
                         @MetadataValue(CORRELATION_ID) String correlationId) {
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
  public Customer handle(FirstNameChanged event, Customer state) {
    return state.toBuilder()
        .firstName(event.getFirstName())
        .build();
  }

  @ApplyEvent
  public Customer handle(LastNameChanged event, Customer state) {
    return state.toBuilder()
        .lastName(event.getLastName())
        .build();
  }

  @ApplyEvent
  public Customer handle(CreditsAdded event, Customer state) {
    return state.toBuilder()
        .credits(state.getCredits() + event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(CreditsIssued event, Customer state) {
    return state.toBuilder()
        .credits(state.getCredits() - event.getAmount())
        .build();
  }

  @ApplyEvent
  public Customer handle(CustomerDeleted event, Customer state) {
    return null;
  }
}
