package io.github.alikelleci.easysourcing.core.example.handlers;

import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.easysourcing.core.example.domain.CustomerEvent.LastNameChanged;
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

