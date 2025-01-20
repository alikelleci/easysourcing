package io.github.alikelleci.easysourcing.example.handlers;

import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.CreditsAdded;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.CreditsIssued;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.CustomerCreated;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.easysourcing.example.domain.CustomerEvent.LastNameChanged;
import io.github.alikelleci.easysourcing.messaging.eventhandling.annotations.HandleEvent;
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

