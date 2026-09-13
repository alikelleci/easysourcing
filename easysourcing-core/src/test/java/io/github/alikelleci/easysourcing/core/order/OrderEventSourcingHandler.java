package io.github.alikelleci.easysourcing.core.order;

import io.github.alikelleci.easysourcing.core.common.annotations.MessageId;
import io.github.alikelleci.easysourcing.core.common.annotations.MetadataValue;
import io.github.alikelleci.easysourcing.core.common.annotations.Timestamp;
import io.github.alikelleci.easysourcing.core.messaging.Metadata;
import io.github.alikelleci.easysourcing.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderDelivered;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.easysourcing.core.order.OrderEvent.OrderShipped;

import java.time.Instant;

import static io.github.alikelleci.easysourcing.core.messaging.Metadata.CORRELATION_ID;

public class OrderEventSourcingHandler {

  @ApplyEvent
  public Order apply(OrderPlaced event, Order state,
                     Metadata metadata,
                     @Timestamp Instant timestamp,
                     @MessageId String messageId,
                     @MetadataValue(CORRELATION_ID) String correlationId) {
    return Order.builder()
        .id(event.getId())
        .customer(event.getCustomer())
        .shippingAddress(event.getShippingAddress())
        .couponCode(event.getCouponCode())
        .status("PLACED")
        .placedAt(timestamp)
        .build();
  }

  @ApplyEvent
  public Order apply(OrderConfirmed event, Order state) {
    return state.toBuilder()
        .status("CONFIRMED")
        .build();
  }

  @ApplyEvent
  public Order apply(OrderShipped event, Order state) {
    return state.toBuilder()
        .status("SHIPPED")
        .trackingNumber(event.getTrackingNumber())
        .build();
  }

  @ApplyEvent
  public Order apply(OrderDelivered event, Order state) {
    return state.toBuilder()
        .status("DELIVERED")
        .build();
  }

  @ApplyEvent
  public Order apply(OrderCancelled event, Order state) {
    return null; // aggregate removed on cancellation
  }
}
