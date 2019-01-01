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

import java.util.Map;

import net.opentsdb.utils.Config;

/**
 * The configuration class for the Kafka Consumer
 */
public class KafkaRpcPluginConfig extends Config {

  // //////// Configuration Options /////////
  public static final String PLUGIN_PROPERTY_BASE = "KafkaRpcPlugin.";
  public static final String KAFKA_CONFIG_PREFIX = PLUGIN_PROPERTY_BASE + "kafka.";
  public static final int DEFAULT_CONSUMER_RATE = 70000; // cannot be zero
  public static final int DEFAULT_CONSUMER_THREADS = 2;
  public static final int DEFAULT_THREAD_CHECK_INTERVAL = 60000;
  public static final long DEFAULT_REQUEUE_DELAY_MS = 300000;
  public static final String METRIC_AGG_FREQUENCY = 
      PLUGIN_PROPERTY_BASE + "metric_agg_frequency";
  public static final int DEFAULT_METRIC_AGG_FREQUENCY = 60000;
  
  // KAFKA
  public static final String AUTO_COMMIT_INTERVAL_MS = 
      "auto.commit.interval.ms";
  public static final String AUTO_COMMIT_ENABLE = "auto.commit.enable";
  public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
  public static final String REBALANCE_BACKOFF_MS = "rebalance.backoff.ms";
  public static final String REBALANCE_RETRIES = "rebalance.retries.max";
  public static final String ZOOKEEPER_CONNECTION_TIMEOUT_MS =
      "zookeeper.connection.timeout.ms";
  public static final String ZOOKEEPER_SESSION_TIMEOUT_MS = 
      "zookeeper.session.timeout.ms";
  
  // KAFKA DEFAULTS
  public static final int AUTO_COMMIT_INTERVAL_DEFAULT = 5000;
  public static final String AUTO_COMMIT_ENABLE_DEFAULT = "true";
  public static final String AUTO_OFFSET_RESET_DEFAULT = "smallest";
  public static final int REBALANCE_BACKOFF_MS_DEFAULT = 60000;
  public static final int REBALANCE_RETRIES_DEFAULT = 20;
  public static final int ZK_CONNECTION_TIMEOUT_DEFAULT = 60000;
  public static final int ZK_SESSION_TIMEOUT_DEFAULT = 60000;
  
  // Producer defaults for the requeue
  public static final String REQUEUE_CONFIG_PREFIX = PLUGIN_PROPERTY_BASE + "seh";
  public static final String KAFKA_TOPIC_PREFIX = REQUEUE_CONFIG_PREFIX + ".topic";
  
  // Kafka pass through values
  public static final String KAFKA_BROKERS = KAFKA_CONFIG_PREFIX + 
      "metadata.broker.list";
  public static final String REQUIRED_ACKS = KAFKA_CONFIG_PREFIX + 
      "request.required.acks";
  public static final String REQUEST_TIMEOUT = KAFKA_CONFIG_PREFIX + 
      "request.timeout.ms";
  public static final String MAX_RETRIES = KAFKA_CONFIG_PREFIX + 
      "send.max_retries";
  public static final String PARTITIONER_CLASS = KAFKA_CONFIG_PREFIX + 
      "partitioner.class";
  public static final String PRODUCER_TYPE = KAFKA_CONFIG_PREFIX + 
      "producer.type";
  public static final String KEY_SERIALIZER = KAFKA_CONFIG_PREFIX + 
      "key.serializer.class";
  
  /**
   * Default ctor
   * @param parent The configuration we're basing this config on
   */
  public KafkaRpcPluginConfig(final Config parent) {
    super(parent);
    setLocalDefaults();
  }

  /** @return The configured or default thread monitor interval in ms */
  public int threadCheckInterval() {
    if (hasProperty(PLUGIN_PROPERTY_BASE + "threadInterval")) {
      return getInt(PLUGIN_PROPERTY_BASE + "threadInterval");
    }
    return DEFAULT_THREAD_CHECK_INTERVAL;
  }
  
  /**
   * Sets Kafka config defaults in the map.
   */
  protected void setLocalDefaults() {
    default_map.put(KAFKA_CONFIG_PREFIX + AUTO_COMMIT_INTERVAL_MS, 
        Integer.toString(AUTO_COMMIT_INTERVAL_DEFAULT));
    default_map.put(KAFKA_CONFIG_PREFIX + AUTO_COMMIT_ENABLE, 
        AUTO_COMMIT_ENABLE_DEFAULT);
    default_map.put(KAFKA_CONFIG_PREFIX + AUTO_OFFSET_RESET, 
        AUTO_OFFSET_RESET_DEFAULT);
    default_map.put(KAFKA_CONFIG_PREFIX + REBALANCE_BACKOFF_MS, 
        Integer.toString(REBALANCE_BACKOFF_MS_DEFAULT));
    default_map.put(KAFKA_CONFIG_PREFIX + REBALANCE_RETRIES, 
        Integer.toString(REBALANCE_RETRIES_DEFAULT));
    default_map.put(KAFKA_CONFIG_PREFIX + ZOOKEEPER_SESSION_TIMEOUT_MS, 
        Integer.toString(ZK_SESSION_TIMEOUT_DEFAULT));
    default_map.put(KAFKA_CONFIG_PREFIX + ZOOKEEPER_CONNECTION_TIMEOUT_MS, 
        Integer.toString(ZK_CONNECTION_TIMEOUT_DEFAULT));
    default_map.put(METRIC_AGG_FREQUENCY, 
        Integer.toString(DEFAULT_METRIC_AGG_FREQUENCY));
    
    default_map.put(REQUIRED_ACKS,  "0");
    default_map.put(REQUEST_TIMEOUT, "10000");
    default_map.put(MAX_RETRIES, "1000");
    default_map.put(PRODUCER_TYPE, "async");
    default_map.put(KEY_SERIALIZER, "kafka.serializer.StringEncoder");
    default_map.put(PARTITIONER_CLASS, 
        "net.opentsdb.tsd.KafkaSimplePartitioner");
    
    for (Map.Entry<String, String> entry : default_map.entrySet()) {
      if (!hasProperty(entry.getKey())) {
        overrideConfig(entry.getKey(), entry.getValue());
      }
    }
  }
}
