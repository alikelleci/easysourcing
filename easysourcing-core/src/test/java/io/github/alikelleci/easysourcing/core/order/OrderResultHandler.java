package io.github.alikelleci.easysourcing.core.order;

import io.github.alikelleci.easysourcing.core.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.easysourcing.core.order.OrderCommand.CancelOrder;
import io.github.alikelleci.easysourcing.core.order.OrderCommand.ConfirmOrder;
import io.github.alikelleci.easysourcing.core.order.OrderCommand.DeliverOrder;
import io.github.alikelleci.easysourcing.core.order.OrderCommand.PlaceOrder;
import io.github.alikelleci.easysourcing.core.order.OrderCommand.ShipOrder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderResultHandler {

  @HandleResult
  public void handle(PlaceOrder command) {
  }

  @HandleResult
  public void handle(ConfirmOrder command) {
  }

  @HandleResult
  public void handle(ShipOrder command) {
  }

  @HandleResult
  public void handle(DeliverOrder command) {
  }

  @HandleResult
  public void handle(CancelOrder command) {
  }

}

