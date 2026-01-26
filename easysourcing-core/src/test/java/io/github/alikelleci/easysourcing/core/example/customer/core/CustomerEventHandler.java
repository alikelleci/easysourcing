package io.github.alikelleci.easysourcing.core.example.customer.core;

import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CreditsAdded;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CreditsIssued;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerEvent.LastNameChanged;
import io.github.alikelleci.easysourcing.core.messaging.eventhandling.annotations.HandleEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerEventHandler {

  @HandleEvent
  public void handle(CustomerCreated event) {
  }

  @HandleEvent
  public void handle(FirstNameChanged event) {
  }

  @HandleEvent
  public void handle(LastNameChanged event) {
  }

  @HandleEvent
  public void handle(CreditsAdded event) {
  }

  @HandleEvent
  public void handle(CreditsIssued event) {
  }

  @HandleEvent
  public void handle(CustomerDeleted event) {
  }
}

