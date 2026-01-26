package io.github.alikelleci.easysourcing.core.example.customer.core;

import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.AddCredits;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.ChangeFirstName;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.ChangeLastName;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.CreateCustomer;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.DeleteCustomer;
import io.github.alikelleci.easysourcing.core.example.customer.shared.CustomerCommand.IssueCredits;
import io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations.HandleResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CustomerResultHandler {

  @HandleResult
  public void handle(CreateCustomer command) {
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

