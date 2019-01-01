// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.tsd;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AtomicDouble;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.KafkaRpcPluginThread.CounterType;

public class KafkaRpcPlugin extends RpcPlugin implements TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      KafkaRpcPlugin.class);

  public static KafkaRpcPlugin KAFKA_RPC_REFERENCE = null;
  
  private final String host;
  
  private ExecutorService thread_pool;
  
  private List<KafkaRpcPluginGroup> consumer_groups;
  
  private KafkaRpcPluginConfig config;
  
  private boolean track_metric_prefix;
  
  private TSDB tsdb;
  
  private final ConcurrentHashMap<String, 
    Map<String, AtomicLong>> namespace_counters;
  
  private final Map<String, AtomicLong> totals_counters;
  private final AtomicLong messages_received = new AtomicLong();
  private final AtomicLong datapoints_received = new AtomicLong();
  private final AtomicLong deserialization_errors = new AtomicLong();
  private final AtomicLong restarts = new AtomicLong();
  private final AtomicLong rebalance_failures = new AtomicLong();
  private final AtomicDouble cumulative_rate_delay = new AtomicDouble();
  private final AtomicDouble kafka_wait_time = new AtomicDouble();
  
  public KafkaRpcPlugin() {
    try {
      host = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Unable to get hostname!", e);
      throw new RuntimeException("WTF? Shouldn't be here", e);
    }
    thread_pool = Executors.newCachedThreadPool();
    namespace_counters = new ConcurrentHashMap<String, Map<String, AtomicLong>>(
        CounterType.values().length);
    totals_counters = Maps.newHashMapWithExpectedSize(CounterType.values().length);
  }
  
  @Override
  public void initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    config = new KafkaRpcPluginConfig(tsdb.getConfig());
    consumer_groups = createConsumerGroups();
    LOG.info("Launching " + consumer_groups.size() + " Kafka consumer groups...");
    for (final KafkaRpcPluginGroup group : consumer_groups) {
      group.start();
    }
    LOG.info("Launched " + consumer_groups.size() + " Kafka consumer groups");
    tsdb.getTimer().newTimeout(this, 100, TimeUnit.MILLISECONDS);
    // Sync just in case the HTTP plugin loads or tries to fetch stats before 
    // we finish initializing.
    synchronized (tsdb) {
      KAFKA_RPC_REFERENCE = this;
    }
    LOG.info("Initialized KafkaRpcPlugin.");
  }

  @Override
  public Deferred<Object> shutdown() {
    LOG.info("Shutting down KafkaRpcPlugin.");
    try {
      if (consumer_groups != null) {
        for (final KafkaRpcPluginGroup group : consumer_groups) {
          try {
            group.shutdown();
          } catch (Throwable e) {
            LOG.error("Failed shutting down Kafka Consumer: " + group 
                + ". Continuing with shutdown.", e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Uncaught exception during shutdown", e);
      return Deferred.fromError(e);
    } catch (Throwable e) {
      LOG.error("Fatal exception in Kafka thread: " + this, e);
      return Deferred.fromError(new RuntimeException("WTF? Unexpected exception "
          + "shutting down Kafka consumer.", e));
    }
    LOG.info("Successfully shutdown KafkaRpcPlugin.");
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "2.4.0";
  }

  @Override
  public void collectStats(final StatsCollector collector) {
    for (final Entry<String, AtomicLong> entry : totals_counters.entrySet()) {
      collector.record("KafkaRpcPlugin." + entry.getKey(), entry.getValue().get());
    }
  }

  /**
   * Returns the reference for the shared namespace counters map for updating.
   * @return The master counters map.
   */
  public ConcurrentMap<String, Map<String, AtomicLong>> getNamespaceCounters() {
    return namespace_counters;
  }
  
  public boolean trackMetricPrefix() {
    return track_metric_prefix;
  }
  
  KafkaRpcPluginConfig getConfig() {
    return config;
  }
    
  ExecutorService getKafkaPool() {
    return thread_pool;
  }
  
  TSDB getTSDB() {
    return tsdb;
  }
  
  String getHost() {
    return host;
  }

  /**
   * Creates a set of kafka consumer groups
   * @throws IllegalArgumentException if a group couldn't be set up due to
   * missing configuration parameters.
   */
  List<KafkaRpcPluginGroup> createConsumerGroups() {
    final String group_list = config.getString(
        KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + "groups");
    if (group_list == null || group_list.isEmpty()) {
        throw new IllegalArgumentException("Missing " + 
            KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + "groups");
    }
    final String[] groups = group_list.split(",");
    
    final List<KafkaRpcPluginGroup> consumerGroups = 
        new ArrayList<KafkaRpcPluginGroup>(groups.length);
    for (final String group : groups) {
        final KafkaRpcPluginGroup consumerGroup = 
            new KafkaRpcPluginGroup(this, group);
        consumerGroups.add(consumerGroup);
    }
    return consumerGroups;
  }

  /**
   * This is the TimerTask that runs periodically to perform metrics
   * aggregation into the totals map.
   */
  @Override
  public void run(final Timeout timeout) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running metric aggregation in Kafka RPC plugin.");
    }
    
    try {
      final Map<String, Long> temp_totals_counters = 
          new HashMap<String, Long>(CounterType.values().length);  
      
      long tempMessageReceived = 0;
      long tempDatapointsReceived = 0;
      long tempDeserializationError = 0;
      long tempRestarts = 0;
      long tempRebalanceFailures = 0;
      double tempCumulativeRateDelay = 0;
      double tempKafkaWaitTime = 0;
      
      for (final KafkaRpcPluginGroup group : consumer_groups) {
        //group.getNamespaceCounters(tempNamespaceCounters);
        tempMessageReceived += group.getMessagesReceived();
        tempDatapointsReceived += group.getDatapointsReceived();
        tempDeserializationError += group.getDeserializationErrors();
        tempRestarts += group.getRestarts();
        tempRebalanceFailures += group.getRebalanceFailures();
        tempCumulativeRateDelay += group.getCumulativeRateDelay();
        tempKafkaWaitTime += group.getKafkaWaitTime();
      }
      
      messages_received.set(tempMessageReceived);
      datapoints_received.set(tempDatapointsReceived);
      deserialization_errors.set(tempDeserializationError);
      restarts.set(tempRestarts);
      rebalance_failures.set(tempRebalanceFailures);
      cumulative_rate_delay.set(tempCumulativeRateDelay);
      kafka_wait_time.set(tempKafkaWaitTime);
      
      // agg the namespace counters into total
      for (final Entry<String, Map<String, AtomicLong>> namespaceCountersMap : 
          namespace_counters.entrySet()) {
        for (final Entry<String, AtomicLong> counter : 
            namespaceCountersMap.getValue().entrySet()) {
          Long val = temp_totals_counters.get(namespaceCountersMap.getKey());
          if (val == null) {
            temp_totals_counters.put(namespaceCountersMap.getKey(), counter.getValue().get());
          } else {
            temp_totals_counters.put(namespaceCountersMap.getKey(), counter.getValue().get() + val);
          }
        }
      }
      
      // replace the master totals now
      for (Entry<String, Long> total : temp_totals_counters.entrySet()) {
        AtomicLong value = totals_counters.get(total.getKey());
        if (value == null) {
          value = new AtomicLong();
          totals_counters.put(total.getKey(), value);
        }
        value.set(total.getValue());
      }
    } catch (Exception e) {
      LOG.error("Unexpected exception in the Metrics thread", e);
    } catch (Throwable e) {
      LOG.error("Fatal exception in the Metrics thread", e);
      System.exit(1);
    }
    
    // always remember to reschedule!
    tsdb.getTimer().newTimeout(this, config.getLong(
        KafkaRpcPluginConfig.METRIC_AGG_FREQUENCY), 
        TimeUnit.MILLISECONDS);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Completed metric aggregation in Kafka RPC plugin.");
    }
  }

  public Map<String, Double> getRates() {
    final Map<String, Double> map = 
        new HashMap<String, Double>(consumer_groups.size());
    for (final KafkaRpcPluginGroup group : consumer_groups) {
      map.put(group.getGroupID(), group.getRate());
    }
    return map;
  }
  
  public void setRate(final String group_id, final double rate) {
    if (group_id == null || group_id.isEmpty()) {
      throw new IllegalArgumentException("GroupID cannot be null or empty");
    }
    if (rate < 0) {
      throw new IllegalArgumentException("Rate cannot be less than zero: " + rate);
    }
    
    for (final KafkaRpcPluginGroup group : consumer_groups) {
      if (group.getGroupID().toLowerCase().equals(group_id.toLowerCase())) {
        group.setRate(rate);
        return;
      }
    }
    
    throw new IllegalArgumentException("GroupID " + group_id + " was not found");
  }
  
  public final Map<String, Map<Integer, Map<String, Double>>> getPerThreadStats() {
    final Map<String, Map<Integer, Map<String, Double>>> map = 
        new HashMap<String, Map<Integer, Map<String, Double>>>();
    for (final KafkaRpcPluginGroup group : consumer_groups) {
      map.put(group.getGroupID(), group.getPerThreadMetrics());
    }
    return map;
  }
}
