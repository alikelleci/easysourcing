package com.github.easysourcing.support.serializer;

import com.github.easysourcing.messages.Message;
import com.github.easysourcing.messages.aggregates.Aggregate;
import com.github.easysourcing.messages.commands.Command;
import com.github.easysourcing.messages.events.Event;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CustomSerdes {

  public static final class MessageSerde extends Serdes.WrapperSerde<Message> {
    public MessageSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(Message.class));
    }
  }

  public static Serde<Message> Message() {
    return new MessageSerde();
  }

  public static final class CommandSerde extends Serdes.WrapperSerde<Command> {
    public CommandSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(Command.class));
    }
  }

  public static Serde<Command> Command() {
    return new CommandSerde();
  }

  public static final class EventSerde extends Serdes.WrapperSerde<Event> {
    public EventSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(Event.class));
    }
  }

  public static Serde<Event> Event() {
    return new EventSerde();
  }

  public static final class AggregateSerde extends Serdes.WrapperSerde<Aggregate> {
    public AggregateSerde() {
      super(new JsonSerializer<>(), new JsonDeserializer<>(Aggregate.class));
    }
  }

  public static Serde<Aggregate> Aggregate() {
    return new AggregateSerde();
  }
}
