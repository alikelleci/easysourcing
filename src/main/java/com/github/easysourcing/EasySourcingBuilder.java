package com.github.easysourcing;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EasySourcingBuilder {

  private EasySourcing easySourcing;

  public EasySourcingBuilder(Config config) {
    this.easySourcing = new EasySourcing(config);
  }

  public EasySourcingBuilder registerHandler(Object handler) {
    easySourcing.registerHandler(handler);
    return this;
  }

  public EasySourcing build() {
    return easySourcing;
  }


}
