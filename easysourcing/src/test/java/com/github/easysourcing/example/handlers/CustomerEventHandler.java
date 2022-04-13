package com.github.easysourcing.example.handlers;

import com.github.easysourcing.example.domain.CustomerEvent.CreditsAdded;
import com.github.easysourcing.example.domain.CustomerEvent.CreditsIssued;
import com.github.easysourcing.example.domain.CustomerEvent.CustomerCreated;
import com.github.easysourcing.example.domain.CustomerEvent.CustomerDeleted;
import com.github.easysourcing.example.domain.CustomerEvent.FirstNameChanged;
import com.github.easysourcing.example.domain.CustomerEvent.LastNameChanged;
import com.github.easysourcing.messaging.Metadata;
import com.github.easysourcing.messaging.eventhandling.annotations.HandleEvent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerEventHandler {

  @HandleEvent
  public void handle(CustomerCreated event, Metadata metadata) {
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

