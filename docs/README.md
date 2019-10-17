# Introduction
EasySourcing is an API for building high performance Event Driven and Event Sourcing applications. It is built on top of Kafka Streams and uses Kafka as backbone. This means that applications built with EasySourcing are highly scalable and fault-tolerant.



 - - - -

# Quick Start


- - - -

## Maven
Add this to your pom.xml:

```javascript
<dependency>
    <groupId>com.github.alikelleci</groupId>
    <artifactId>easysourcing</artifactId>
    <version>0.0.11</version>
</dependency>
 ``` 
- - - -

## Spring Boot


- - - -


# Reference

 - - - -
## Aggregate
An Aggregate is a regular java object, which contains state and methods to alter that state. In the following example Customer is considered as an aggregate: 


```javascript
@Value
@Builder(toBuilder = true)
public class Customer {

  @AggregateId  // 1
  private String id;  
  private String firstName;
  private String lastName;
  private String address;

  @ApplyEvent   // 2
  public Customer apply(CustomerCreated event) {
    return this.toBuilder()
        .id(event.getCustomerId())
        .firstName(event.getFirstName())
        .lastName(event.getLastName())
        .address(event.getAddress())
        .build();
  }

  @ApplyEvent   
  public Customer apply(CustomerAddressChanged event) {
    return this.toBuilder()
        .address(event.getAddress())    // 3
        .build();
  }
}
 ``` 

In the code snippet above we use Lombok annotations to reduce some of the boilerplate code. We also see some other interesting parts of the code which is marked with a java comment:

1. We annotated the customer id with `@AggregateId`. This field is required, as without it EasySourcing will not know to which Aggregate a given Command is targeted.

2. Methods annotated with `@ApplyEvent` gets invoked when the Aggregate is 'sourced from its events'. In this case, whenever a CustomerCreated event occurs, this method gets invoked. Because the CustomerCreated event is a 'creation' event, we set the customer id here and populate all other  fields.

  The return value of this method should always be the **type of the aggregate**. In this case, we return a new copy of the Customer object with updated state. Although it is not required to return a new copy, it is recommended that you use immutable objects as much as possible. 


3. This method is also annotated with `@ApplyEvent`. In here, we listen for CustomerAddressChanged events. When this event occurs, we only update the corresponding field. Notice that we don't need to set the customer id here.

> **When designing your application, it is important that you only have one event sourcing method per event type. If your application has multiple event sourcing methods for the same event type, then only one method will be invoked.**

## Commands


### Command handling
An aggregate has one or more commands associated with it, and each of these commands, once invoked, may result in one or more events being generated.

The command handler’s job is to validate an incoming command, and either accept or reject it. If the command is accepted it must dispatch one or more events. If it is rejected, or command processing fails, it should return with one or more reasons for failure.

An aggregate requires a command handler for each command that operates on it. You can create a command handler by simply annotating your command handling method with `@HandleCommand`:

```javascript
@HandleCommand
public OrderCreated handle(CreateOrder command) {
    return new OrderCreated();
}
 ``` 
In the example above, an OrderCreated event is dispatched when a CreateOrder commands comes in. You can also dispatch multiple events by simply returning a list of events:

```javascript
@HandleCommand
public List<OrderEvent> handle(CreateOrder command) {
    List<OrderEvent> events = new ArrayList<>();
    commands.add(new OrderCreated());
    commands.add(new OrderConfirmed());

    return events;
}
 ``` 

When dispatching multiple events, the order is preserved for events with the **same** aggregate identifier. This means that ordering is **not** guaranteed for events with different identifiers.


> **Marking a method as Void will not dispatch any events. This is also the case when returning a null-value. A method returning a value or a list of values will dispatch single or multiple events.**

### Dispatching commands
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

 - - - -

## Events
### Event handling 
Methods annotated with `@HandleEvent` will get invoked when the corresponding event occurs.
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.


```javascript
@HandleEvent
public void handle(OrderCreated event) {
    // Perform some action

}
 ``` 

> NOTE: Lorem ipsum dolor sit amet

```javascript
@HandleEvent
public Object handle(OrderCreated event) {
    return new ShipOrder();
}
 ``` 

> NOTE: Lorem ipsum dolor sit amet


```javascript
@HandleEvent
public List<Object> handle(OrderCreated event) {
    List<Object> commands = new ArrayList<>();
    commands.add(new Command1());
    commands.add(new Command2());

    return commands
}
 ``` 

> NOTE: Lorem ipsum dolor sit amet


### Dispatching events
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.


### Event sourcing
Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
