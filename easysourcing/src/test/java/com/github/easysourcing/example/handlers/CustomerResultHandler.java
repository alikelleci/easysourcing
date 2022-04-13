package com.github.easysourcing.example.handlers;

import com.github.easysourcing.example.domain.CustomerCommand.AddCredits;
import com.github.easysourcing.example.domain.CustomerCommand.ChangeFirstName;
import com.github.easysourcing.example.domain.CustomerCommand.ChangeLastName;
import com.github.easysourcing.example.domain.CustomerCommand.CreateCustomer;
import com.github.easysourcing.example.domain.CustomerCommand.DeleteCustomer;
import com.github.easysourcing.example.domain.CustomerCommand.IssueCredits;
import com.github.easysourcing.messaging.Metadata;
import com.github.easysourcing.messaging.resulthandling.annotations.HandleResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerResultHandler {

  @HandleResult
  public void handle(CreateCustomer command, Metadata metadata) {
  }

  @HandleResult
  public void handle(ChangeFirstName command) {
  }

  @HandleResult
  public void handle(ChangeLastName command) {
  }

  @HandleResult
  public void handle(AddCredits event) {
  }

  @HandleResult
  public void handle(IssueCredits event) {
  }

  @HandleResult
  public void handle(DeleteCustomer event) {
  }

}

