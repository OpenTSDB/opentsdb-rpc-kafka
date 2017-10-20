// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

/**
 * A group of consumers that pull from a list of one or more topics. This group
 * monitors the running consumers periodically and restarts them as necessary.
 */
public class KafkaRpcPluginGroup implements TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(
      KafkaRpcPluginGroup.class);
  
  /**
   * An enumerator that maps TSDB consumer types to strings. This lets the
   * consumer know how to process the messages from it's topics.
   */
  public enum TsdbConsumerType {
    RAW("Raw"),
    REQUEUE_RAW("RequeueRaw"),
    ROLLUP("Rollup"),
    REQUEUE_ROLLUP("RequeueRollup"),
    UID_ABUSE("UIDAbuse");
    
    final String name;
    TsdbConsumerType(final String name) {
      this.name = name;
    }
    
    public String toString() {
      return name;
    }
  }
  
  private final KafkaRpcPluginConfig config;
  private final String group_id;
  private final RateLimiter rate_limiter;
  private final List<KafkaRpcPluginThread> kafka_consumers;
  private final int num_threads;
  private final String topics;
  private final HashedWheelTimer timer;
  private final ExecutorService pool;
  private final TsdbConsumerType consumer_type;
  private final KafkaRpcPlugin parent;
  
  private final AtomicLong restarts = new AtomicLong();
  private final AtomicLong rebalance_failures = new AtomicLong();
  
  private double current_rate;
  
  /**
   * Default CTor
   * @param parent The parent object to pull shared objects from
   * @param groupID The group ID used to subscribe to Kafka
   */
  public KafkaRpcPluginGroup(final KafkaRpcPlugin parent, final String groupID) {
    this.parent = parent;
    this.group_id = groupID;
    config = parent.getConfig();
    timer = (HashedWheelTimer) parent.getTSDB().getTimer();
    pool = parent.getKafkaPool();
    
    if (groupID == null || groupID.isEmpty()) {
      throw new IllegalArgumentException("Missing group name");
    }
    
    topics = config.getString(
        KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + groupID + ".topics");
    if (topics == null || topics.isEmpty()) {
      throw new IllegalArgumentException("Empty topic filter for group " + groupID);
    }
    
    final String type = config.getString(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE 
        + groupID + ".consumerType");
    if (type == null || type.isEmpty()) {
      throw new IllegalArgumentException("Missing consumer type for group " + groupID);
    }
    consumer_type = TsdbConsumerType.valueOf(type.toUpperCase());
    
    if (config.hasProperty(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + groupID + ".rate")) {
      current_rate = config.getInt(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + 
          groupID + ".rate");
      if (current_rate > 0) {
        // can't set the rate to zero
        rate_limiter = RateLimiter.create(current_rate);
      } else {
        rate_limiter = RateLimiter.create(KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE);
      }
    } else {
      current_rate = KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE;
      rate_limiter = RateLimiter.create(KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE);
    }
    
    num_threads = 
        config.hasProperty(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + groupID + ".threads")
        ? config.getInt(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + groupID + ".threads") 
            : KafkaRpcPluginConfig.DEFAULT_CONSUMER_THREADS;
    kafka_consumers = new ArrayList<KafkaRpcPluginThread>(num_threads);
    
    for (int i = 0; i < num_threads; i++) {
      kafka_consumers.add(new KafkaRpcPluginThread(this, i, topics));
    }
    
    timer.newTimeout(this, config.threadCheckInterval(), TimeUnit.MILLISECONDS);
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("groupID=")
      .append(group_id)
      .append(", type=")
      .append(consumer_type)
      .append(", currentRate=")
      .append(current_rate)
      .append(", numThreads=")
      .append(num_threads)
      .append(", topics=")
      .append(topics);
    return buf.toString();
  }
  
  /**
   * Starts the threads given the thread pool
   */
  public void start() {
    if (current_rate > 0) {
      for (final Thread t : kafka_consumers) {
        pool.execute(t);
      }
    }
  }
  
  /** Gracefully shuts down all of the consumer threads */
  public void shutdown() {
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      consumer.shutdown();
    }
  }
  
  /** @return the group ID to use as the consumer group */
  public String getGroupID() {
    return group_id;
  }
  
  /** @return the rate limiter for this group. Shared amongst partitions */
  public RateLimiter getRateLimiter() {
    return rate_limiter;
  }
  
  /** @return the parent object that controls this group */
  public KafkaRpcPlugin getParent() {
    return parent;
  }
  
  /** @return the consumer type */
  public TsdbConsumerType getConsumerType() {
    return consumer_type;
  }

  /** Increments the rebalance failure counter */
  public void incrementRebalanceFailures() {
    rebalance_failures.incrementAndGet();
  }
  
  /** @return the number of thread restarts for all threads */
  public long getRestarts() {
    return restarts.get();
  }
  
  /** @return the number of rebalance failures across all threads */
  public long getRebalanceFailures() {
    return rebalance_failures.get();
  }
  
  /** @return the number of messages received across all threads */
  public long getMessagesReceived() {
    long temp = 0;
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      temp += consumer.getMessagesReceived();
    }
    return temp;
  }

  /** @return the number of datapoints received across all threads */
  public long getDatapointsReceived() {
    long temp = 0;
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      temp += consumer.getDatapointsReceived();
    }
    return temp;
  }
  
  /** @return the number of seconds spent waiting on the rate limiter */
  public double getCumulativeRateDelay() {
    double temp = 0;
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      temp += consumer.getCumulativeRateDelay();
    }
    return temp;
  }
  
  /** @return the number of ms spent waiting for messages from Kafka */
  public double getKafkaWaitTime() {
    double temp = 0;
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      temp += consumer.getKafkaWaitTime();
    }
    return temp;
  }
  
  /** @return the number of deserialization errors across all threads */
  public long getDeserializationErrors() {
    long temp = 0;
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      temp += consumer.getDeserializationErrors();
    }
    return temp;
  }
  
  /**
   * Populates the given map with the aggregate of counters across all threads.
   * @param counters The map to populate with results
   */
  public void getNamespaceCounters(final Map<String, Map<String, Long>> counters) {
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      final Map<String, Map<String, AtomicLong>> threadCounters = 
          consumer.getNamespaceCounters();
      
      for (final Entry<String, Map<String, AtomicLong>> ctr : 
        threadCounters.entrySet()) {
        
        Map<String, Long> tempMap = counters.get(ctr.getKey());
        if (tempMap == null) {
          tempMap = new HashMap<String, Long>(1);
          counters.put(ctr.getKey(), tempMap);
        }
        
        for (final Entry<String, AtomicLong> namespace : ctr.getValue().entrySet()) {
          final Long value = tempMap.get(namespace.getKey());
          if (value == null) {
            tempMap.put(namespace.getKey(), namespace.getValue().get());
          } else {
            tempMap.put(namespace.getKey(), value + namespace.getValue().get());
          }
        }
      }
    }
  }
  
  /** @return a map of per thread statistics */
  public Map<Integer, Map<String, Double>> getPerThreadMetrics() {
    final Map<Integer, Map<String, Double>> map = 
        new HashMap<Integer, Map<String, Double>>();
    for (final KafkaRpcPluginThread consumer : kafka_consumers) {
      final Map<String, Double> thread = new HashMap<String, Double>();
      thread.put("messagesReceived", (double)consumer.getMessagesReceived());
      thread.put("datapointsReceived", (double)consumer.getDatapointsReceived());
      thread.put("cumulativeRateDelay", consumer.getCumulativeRateDelay());
      thread.put("kafkaWaitTime", consumer.getKafkaWaitTime());
      map.put(consumer.threadID(), thread);
    }
    return map;
  }
  
  /** @return the currently configured rate of the group */
  public double getRate() {
    return current_rate == 0 ? 0 : rate_limiter.getRate();
  }
  
  /**
   * Sets the rate limit for the group. If the rate is zero, all consumer threads
   * are killed. If the rate is greater than zero then any stopped consumer threads
   * will be restarted at the next monitoring interval.
   * @param rate The rate to set. Must be zero or greater.
   */
  public void setRate(final double rate) {
    if (rate < 0) {
      throw new IllegalArgumentException("Rate cannot be less than zero " + rate);
    }
    current_rate = rate;
    if (rate == 0) {
      // kill the threads!
      LOG.info("The rate has been set to zero for " + this + ". Killing threads.");
      for (final KafkaRpcPluginThread writer : kafka_consumers) {
        try {
        writer.shutdown();
        } catch (Exception e) {
          LOG.error("Exception shutting down thread " + writer, e);
        }
      }
    } else {
      // limiter requires a positive value, can't set it to 0
      rate_limiter.setRate(rate);
    }
  }
  
  /**
   * Responsible for restarting dead threads. If the current rate is set to zero
   * then the threads will not be restarted.
   */
  public void run(final Timeout timeout) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Running thread monitor on " + this);
    }
    try {
      // only check threads if the rate is greater than zero
      if (current_rate > 0) {
        for (final KafkaRpcPluginThread writer : kafka_consumers) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Writer [" + writer + "] thread state: " + writer.isThreadRunning());
          }
          if (!writer.isThreadRunning()) {
            LOG.warn("Writer [" + writer + "] was terminated, restarting");
            pool.execute(writer);
            restarts.incrementAndGet();
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Consumer group " + this + " has a rate limit of 0, not running");
        }
      }
    } catch (Exception e) {
      LOG.error("Failure while monitoring threads for group: " + this, e);
    } catch (Throwable e) {
      LOG.error("Fatal exception in group thread: " + this, e);
      System.exit(1);
    }
    timer.newTimeout(this, config.threadCheckInterval(), TimeUnit.MILLISECONDS);
  }
}
