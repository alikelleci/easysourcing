package io.github.alikelleci.easysourcing.core.order;

import io.github.alikelleci.easysourcing.core.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderDelivered;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderShipped;

public class OrderEventHandler {

  @HandleEvent
  public void on(OrderPlaced event) { /* insert into read model */ }

  @HandleEvent
  public void on(OrderConfirmed event) { /* update read model */ }

  @HandleEvent
  public void on(OrderShipped event) { /* send shipping notification */ }

  @HandleEvent
  public void on(OrderDelivered event) { /* update read model */ }

  @HandleEvent
  public void on(OrderCancelled event) { /* remove from read model */ }
}

