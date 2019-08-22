package com.easysourcing.api.message.snapshots;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public interface Snapshotable {

}
