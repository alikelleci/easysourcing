package io.github.alikelleci.easysourcing.core.order;

import io.github.alikelleci.easysourcing.core.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.core.common.annotations.AggregateRoot;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@Builder(toBuilder = true)
@AggregateRoot
public class Order {
  @AggregateId
  String id;
  String customer;
  String shippingAddress;
  String couponCode;
  String status;
  String trackingNumber;
  Instant placedAt;
}

