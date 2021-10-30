package com.github.easysourcing.constants;

import com.github.easysourcing.messages.aggregates.Aggregator;
import com.github.easysourcing.messages.commands.CommandHandler;
import com.github.easysourcing.messages.events.EventHandler;
import com.github.easysourcing.messages.results.ResultHandler;
import com.github.easysourcing.messages.snapshots.SnapshotHandler;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.util.HashMap;
import java.util.Map;

public class Handlers {
  public static Map<Class<?>, CommandHandler> COMMAND_HANDLERS = new HashMap<>();
  public static Map<Class<?>, Aggregator> AGGREGATORS = new HashMap<>();
  public static MultiValuedMap<Class<?>, ResultHandler> RESULT_HANDLERS = new ArrayListValuedHashMap<>();
  public static MultiValuedMap<Class<?>, EventHandler> EVENT_HANDLERS = new ArrayListValuedHashMap<>();
  public static MultiValuedMap<Class<?>, SnapshotHandler> SNAPSHOT_HANDLERS = new ArrayListValuedHashMap<>();
}
