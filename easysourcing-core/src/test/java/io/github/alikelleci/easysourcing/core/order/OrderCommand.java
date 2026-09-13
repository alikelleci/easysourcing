package io.github.alikelleci.easysourcing.core.order;

import io.github.alikelleci.easysourcing.core.common.annotations.AggregateId;
import io.github.alikelleci.easysourcing.core.common.annotations.TopicInfo;
import jakarta.validation.constraints.NotBlank;
import lombok.Builder;
import lombok.Value;

@TopicInfo("commands.order")
public interface OrderCommand {

  @Value
  @Builder
  class PlaceOrder implements OrderCommand {
    @AggregateId
    String id;
    @NotBlank
    String customer;
    @NotBlank
    String shippingAddress;
    String couponCode;
  }

  @Value
  @Builder
  class ConfirmOrder implements OrderCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  class ShipOrder implements OrderCommand {
    @AggregateId
    String id;
    @NotBlank
    String trackingNumber;
  }

  @Value
  @Builder
  class DeliverOrder implements OrderCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  class CancelOrder implements OrderCommand {
    @AggregateId
    String id;
    @NotBlank
    String reason;
  }
}

