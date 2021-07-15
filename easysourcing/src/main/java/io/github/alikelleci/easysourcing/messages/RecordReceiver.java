package io.github.alikelleci.easysourcing.messages;

public interface RecordReceiver<T> {

  T receive(String correlationId);

}
