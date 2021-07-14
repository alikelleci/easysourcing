package io.github.alikelleci.easysourcing.messages.commands;

import io.github.alikelleci.easysourcing.Gateway;
import io.github.alikelleci.easysourcing.messages.Metadata;
import io.github.alikelleci.easysourcing.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;

@Slf4j
public class CommandGateway extends Gateway {

  public CommandGateway(Producer<String, Object> producer) {
    super(producer);
  }

  @Override
  public void send(Object payload, Metadata metadata) {
    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), CommonUtils.getAggregateId(payload));
    super.send(payload, metadata);
  }

}
