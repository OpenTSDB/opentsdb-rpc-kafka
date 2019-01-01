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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.Aggregate;
import net.opentsdb.data.Histogram;
import net.opentsdb.data.Metric;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.JSON;

public class KafkaStorageExceptionHandler extends StorageExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(
      KafkaStorageExceptionHandler.class);
  
  /** The type of requeue topics we'll write to */
  public enum KafkaRequeueTopic {
    DEFAULT,
    RAW,
    ROLLUP,
    HISTOGRAM,
    PREAGGREGATE
  }
  
  /** Enums to the topic name maps, populated from the config */
  private final Map<KafkaRequeueTopic, String> topic_name_map;
  
  /** The number of data points pushed to the producer */
  private final Map<KafkaRequeueTopic, AtomicLong> topic_requeued_counters;
  
  /** The number of data points that failed serialization */
  private final Map<KafkaRequeueTopic, AtomicLong> topic_requeued_exception_counters;
  
  private TSDB tsdb;
  
  /** The configuration object loaded from the tsd */
  private KafkaRpcPluginConfig config;
  
  /** A Kafka producer configuration object */
  private ProducerConfig producer_config;
  
  /** A Kafka producer */
  private Producer<String, byte[]> producer;
  
  /**
   * Default ctor
   * @param config The config to pull settings from
   * @throws IllegalArgumentException if a required config value isn't set
   */
  public KafkaStorageExceptionHandler() {
    topic_name_map = new HashMap<KafkaRequeueTopic, String>(
        KafkaRequeueTopic.values().length);
    topic_requeued_counters = new HashMap<KafkaRequeueTopic, AtomicLong>(
        KafkaRequeueTopic.values().length);
    topic_requeued_exception_counters = new HashMap<KafkaRequeueTopic, AtomicLong>(
        KafkaRequeueTopic.values().length);
    for (final KafkaRequeueTopic topic : KafkaRequeueTopic.values()) {
      topic_requeued_counters.put(topic, new AtomicLong());
      topic_requeued_exception_counters.put(topic, new AtomicLong());
    }
  }
  
  /**
   * Closes the producer if it's not null already
   */
  public Deferred<Object> shutdown() {
    LOG.warn("Shutting down Kafka requeue producer");
    if (producer != null) {
      producer.close();
    }
    return Deferred.fromResult(null);
  }
  
  /**
   * Configures the properties object by running through all of the entries in
   * the config class prefixed with the {@code CONFIG_PREFIX} and adding them
   * to the properties list.
   * @throws IllegalArgumentException if a required config is missing
   */
  private void setKafkaConfig() {
    if (!config.hasProperty(KafkaRpcPluginConfig.KAFKA_BROKERS) ||
        config.getString(KafkaRpcPluginConfig.KAFKA_BROKERS) == null ||
        config.getString(KafkaRpcPluginConfig.KAFKA_BROKERS).isEmpty()) {
      throw new IllegalArgumentException("Missing required config object: " + 
          KafkaRpcPluginConfig.KAFKA_BROKERS);
    }
    
    for (final KafkaRequeueTopic t : KafkaRequeueTopic.values()) {
      final String config_name = KafkaRpcPluginConfig.KAFKA_TOPIC_PREFIX + 
          "." + t.name().toLowerCase();
      final String topic = config.getString(config_name);
      if (Strings.isNullOrEmpty(topic)) {
        if (t == KafkaRequeueTopic.DEFAULT) {
          throw new IllegalArgumentException("Missing required default topic: " + 
              config_name);
        }
        continue;
      }
      
      topic_name_map.put(t, config.getString(config_name));
      LOG.info("Mapping topic type " + t + " to topic " + topic_name_map.get(t));
    }
    
    final Properties properties = new Properties();
    for (Map.Entry<String, String> entry : config.getMap().entrySet()) {
      final String key = entry.getKey();
      if (entry.getKey().startsWith(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX)) {
        properties.put(key.substring(
            key.indexOf(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX) + 
            KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX.length()), 
            entry.getValue());
      }
    }
    
    producer_config = new ProducerConfig(properties);
  }
  
  @Override
  public void initialize(TSDB tsdb) {
    this.tsdb = tsdb;
    config = new KafkaRpcPluginConfig(tsdb.getConfig());

    setKafkaConfig();
    producer = new Producer<String, byte[]>(producer_config);
    LOG.info("Initialized kafka requeue publisher.");
  }

  @Override
  public String version() {
    return "2.4.0";
  }

  @Override
  public void collectStats(final StatsCollector collector) {
    for (final Entry<KafkaRequeueTopic, AtomicLong> ctr : 
      topic_requeued_counters.entrySet()) {
      collector.record("plugin.KafkaStorageExceptionHandler.messageRequeuedByType", 
          ctr.getValue().get(), "type=" + ctr.getKey());
    }
    
    for (final Entry<KafkaRequeueTopic, AtomicLong> ctr : 
      topic_requeued_exception_counters.entrySet()) {
      collector.record("plugin.KafkaStorageExceptionHandler.requeueErrorsByType", 
          ctr.getValue().get(), "type=" + ctr.getKey());
    }
  }

  @Override
  public void handleError(final IncomingDataPoint dp, 
                          final Exception exception) {
    KafkaRequeueTopic type = KafkaRequeueTopic.DEFAULT;
    try {
      if (dp == null) {
        throw new IllegalArgumentException("Unable to reque a null data point.");
      }
      
      if (dp instanceof Metric) {
        type = KafkaRequeueTopic.RAW;
      } else if (dp instanceof Aggregate && 
          Strings.isNullOrEmpty(((Aggregate) dp).getInterval())) {
        type = KafkaRequeueTopic.PREAGGREGATE;
      } else if (dp instanceof Aggregate) {
        type = KafkaRequeueTopic.ROLLUP;
      } else if (dp instanceof Histogram) {
        type = KafkaRequeueTopic.HISTOGRAM;
      }
      
      String topic = topic_name_map.get(type);
      if (Strings.isNullOrEmpty(topic)) {
        type = KafkaRequeueTopic.DEFAULT;
        topic = topic_name_map.get(type);
      }
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("Routing SEH message " + dp + " to topic: " + topic);
      }
      
      int hash = dp.getMetric().hashCode() + Objects.hashCode(dp.getTags());
      final KeyedMessage<String, byte[]> data = 
          new KeyedMessage<String, byte[]>(topic, 
              Integer.toString(Math.abs(hash)),
              JSON.serializeToBytes(dp));
      producer.send(data);
      
      final AtomicLong requeued = topic_requeued_counters.get(type);
      if (requeued != null) {
        requeued.incrementAndGet();
      }
    } catch (final Exception ex) {
      LOG.error("Unexpected exception publishing data", ex);
      final AtomicLong requeue_ex = topic_requeued_exception_counters
          .get(type);
      if (requeue_ex != null) {
        requeue_ex.incrementAndGet();
      }
    }
  }

}
