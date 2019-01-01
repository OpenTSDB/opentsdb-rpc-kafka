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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;

import joptsimple.internal.Strings;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import kafka.common.ConsumerRebalanceFailedException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TypedIncomingData;
import net.opentsdb.data.deserializers.Deserializer;
import net.opentsdb.tsd.KafkaRpcPluginGroup.TsdbConsumerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * New and Improved Kafka to TSDB Writer! Now with TSDB Integration!
 * <p>
 * This writer thread will consume directly from Kafka and attempt to write the
 * data point to TSDB via the JAVA API. If the write fails due to an HBase error
 * then the data will be requeued. If the write fails because the data is bad, 
 * the value is logged and bit-bucketed.
 */
public class KafkaRpcPluginThread extends Thread {
  static final String CONSUMER_ID = "consumer.id";
  static final String GROUP_ID = "group.id";
  static final String AGG_TAG = "_aggregate";
  static final String DEFAULT_COUNTER_ID = "null";
  private static final Logger LOG = LoggerFactory.getLogger(
      KafkaRpcPluginThread.class);

  /** Types of metrics we're tracking */
  public enum CounterType {
    ReadRaw("readRawCounter"),
    ReadRequeuedRaw("readRequeueRawCounter"),
    ReadAggregate("readAggregateCounter"),
    ReadRequeuedAggregate("readRequeueAggregateCounter"),
    ReadRollup("readRollupCounter"),
    ReadRequeuedRollup("readRequeueRollupCounter"),
    ReadHistogram("readHistogramCounter"),
    ReadRequeuedHistogram("readRequeueHistogramCounter"),
    StoredRaw("storedRawCounter"),
    StoredRequeuedRaw("storedRequeueRawCounter"),
    StoredAggregate("storedAggregateCounter"),
    StoredRequeuedAggregate("storedRequeueAggregateCounter"),
    StoredRollup("storedRollupCounter"),
    StoredRequeuedRollup("storedRequeueRollupCounter"),
    StoredHistogram("storedHistogramCounter"),
    StoredRequeuedHistogram("storedRequeueHistogramCounter"),
    UnknownMetric("unknownMetricCounter"),
    RequeuedRaw("requeuedRawCounter"),
    RequeuedAggregate("requeuedAggregateCounter"),
    RequeuedRollup("requeuedRollupCounter"),
    RequeuedHistogram("requeuedHistogramCounter"),
    RequeuesDelayed("requeuesDelayedCounter"),
    Exception("exceptionCounter"),
    DroppedRawRollup("droppedRawRollupCounter"),
    StorageException("storageExceptionCounter"),
    PleaseThrottle("pleaseThrottleExceptionCounter"),
    TimeoutException("timeoutExceptionCounter"),
    IllegalArgument("illegalArgumentCounter"),
    UIDCacheMiss("uidCacheMissCounter"),
    NAN("nanCounter"),
    UnknownRollup("unknownRollupCounter"),
    EmptyMessage("emptyMessageCounter"),
    StatusMessage("statusMessageCounter"),
    UnknownException("unknownExceptionCounter"),
    UIDAbuse("uidAbuseCounter");
    
    private final String name;
    
    CounterType(final String theName) {
        name = theName;
    }

    @Override
    public String toString() {
        return name;
    }
  }
  
  private final TSDB tsdb;
  
  /** <counter_type, <NS, count>> */
  private final ConcurrentMap<String, Map<String, AtomicLong>> namespace_counters;
  private final boolean track_metric_prefix;
  private final int thread_id;
  private final KafkaRpcPluginGroup group;
  private final String consumer_id;
  private final TopicFilter topic_filter;
  private final int number_consumer_streams = 1;
  private final RateLimiter rate_limiter;
  private final TsdbConsumerType consumer_type;
  private final long requeue_delay;
  private final AtomicBoolean thread_running = new AtomicBoolean();
  
  private final AtomicLong messagesReceived = new AtomicLong();
  private final AtomicLong datapointsReceived = new AtomicLong();
  private final AtomicLong deserializationErrors = new AtomicLong();
  private final AtomicDouble cumulativeRateDelay = new AtomicDouble();
  private final AtomicDouble kafkaWaitTime = new AtomicDouble();
  private final Deserializer deserializer;
  
  private ConsumerConnector consumer;
  
  /**
   * Default ctor
   * @param group The group object this writer belongs to
   * @param threadID The ID of the thread, an index from 0 to max int
   * @param topics The topic list to subscribe to
   */
  public KafkaRpcPluginThread(final KafkaRpcPluginGroup group, 
      final int threadID, final String topics) {
    if (topics == null || topics.isEmpty()) {
      throw new IllegalArgumentException("Missing topics");
    }
    if (threadID < 0) {
      throw new IllegalArgumentException("Cannot have a negative thread ID: " 
          + threadID);
    }
    if (group.getParent().getTSDB() == null) {
      throw new IllegalArgumentException("Missing TSDB in the group");
    }
    if (group.getRateLimiter() == null) {
      throw new IllegalArgumentException("Missing rate limiter in the group");
    }
    if (group.getGroupID() == null || group.getGroupID().isEmpty()) {
      throw new IllegalArgumentException("Missing group ID");
    }
    if (group.getParent().getHost() == null || 
        group.getParent().getHost().isEmpty()) {
      throw new IllegalArgumentException("Missing host name");
    }
    
    namespace_counters = group.getParent().getNamespaceCounters();
    track_metric_prefix = group.getParent().trackMetricPrefix();
    this.thread_id = threadID;
    this.group = group;
    this.tsdb = group.getParent().getTSDB();
    this.rate_limiter = group.getRateLimiter();
    this.consumer_type = group.getConsumerType();
    thread_running.set(false);

    topic_filter = new Whitelist(topics);
    consumer_id = threadID + "_" + group.getParent().getHost();
    if (consumer_type == TsdbConsumerType.REQUEUE_RAW) {
      if (group.getParent().getConfig().hasProperty(
          KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + "requeueDelay")) {
        requeue_delay = group.getParent().getConfig().getLong(
            KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + "requeueDelay");
      } else {
        requeue_delay = KafkaRpcPluginConfig.DEFAULT_REQUEUE_DELAY_MS;
      }
    } else {
      requeue_delay = 0;
    }
    deserializer = group.getDeserializer();
  }

  @Override
  public String toString() {
    return group.getGroupID() + "_" + consumer_id;
  }

  /**
   * Creates a Kafka consumer ready to read data
   * @return The consumer connector
   * @throws IllegalArgumentException if the config is invalid
   */
  ConsumerConnector buildConsumerConnector() {
    final Properties properties = buildConsumerProperties();
    return kafka.consumer.Consumer
        .createJavaConsumerConnector(new ConsumerConfig(properties));
  }

  /**
   * Helper to build the properties map to pass to the Kafka consumer.
   * This will load anything starting with "KafkaRpcPlugin.kafka" into the
   * map, then override those with any group specific values. 
   * @return Properties for the Kafka consumer
   */
  Properties buildConsumerProperties() {
    final Properties properties = new Properties();
    
    // auto load kafka defaults "yms.kafkaConsumer.kafka.<key>"
    for (Map.Entry<String, String> entry : 
            group.getParent().getConfig().getMap().entrySet()) {
      final String key = entry.getKey();
      if (key.startsWith(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX) && 
          !key.contains(group.getGroupID())) {
        properties.put(key.substring(
            KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX.length()), entry.getValue());
      }
    }
    
    // override with per group configs "KafkaRpcPlugin.<GROUP>.<key>"
    final String groupPrefix = KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
        group.getGroupID() + ".";
    for (Map.Entry<String, String> entry : 
            group.getParent().getConfig().getMap().entrySet()) {
      final String key = entry.getKey();
      if (key.startsWith(groupPrefix)) {
        properties.put(key.substring(groupPrefix.length()), entry.getValue());
      }
    }
    
    // settings from other parts of our code base
    properties.put(GROUP_ID, group.getGroupID());
    properties.put(CONSUMER_ID, consumer_id);
    LOG.info("Initializing consumer config with consumer id " + consumer_id + 
        " and props " + properties);
    return properties;
  }
  
  @Override
  public void run() {
    thread_running.set(true);
    Thread.currentThread().setName(group.getGroupID() + "_" + consumer_id);
    
    try {
      consumer = buildConsumerConnector();
      // We're only fetching one stream at a time per thread so we only care
      // about the first one.
      final KafkaStream<byte[], byte[]> stream = 
          consumer.createMessageStreamsByFilter(topic_filter, 
              number_consumer_streams).get(0);

      final ConsumerIterator<byte[], byte[]> it = stream.iterator();
      int errorCount = 0;
      long nanoCtr = System.nanoTime();
      while (it.hasNext()) {
        try {
          // The following line will block until a message has been read from
          // the stream.
          final MessageAndMetadata<byte[], byte[]> message = it.next();
          long delay = System.nanoTime() - nanoCtr;
          if (nanoCtr > 0) {
            kafkaWaitTime.addAndGet(((double) delay / (double)1000000));
          }
          messagesReceived.incrementAndGet();
          
          // Now that we have received the message, we should record the present
          // system time.
          final long recvTime = System.currentTimeMillis();
          
          switch (consumer_type) {
          case RAW:
          case ROLLUP:
            // Deserialize the event from the received (opaque) message.
            final List<TypedIncomingData> eventList = 
              deserializer.deserialize(this, message.message());
            if (eventList == null) {
              deserializationErrors.incrementAndGet();
              continue;
            }
            
            // I &#9825 Google! It's so easy! No release necessary! Thread Safe!
            final double waiting = rate_limiter.acquire();
            cumulativeRateDelay.addAndGet(waiting);
            datapointsReceived.addAndGet(eventList.size());
            for (TypedIncomingData ev : eventList) {
              ev.processData(this, recvTime);
            }
            break;
          case REQUEUE_RAW:
          case REQUEUE_ROLLUP:
          case UID_ABUSE:
            final List<TypedIncomingData> requeuedList = 
              deserializer.deserialize(this, message.message());
            if (requeuedList == null) {
              deserializationErrors.incrementAndGet();
              continue;
            }
            
            // to avoid tight requeue loops we want to sleep a spell
            // if we receive a data point that was recently added
            // to the queue
            datapointsReceived.addAndGet(requeuedList.size());
            for(TypedIncomingData ev : requeuedList) {
              final long requeueDiff = System.currentTimeMillis() - ev.getRequeueTS();
              if (requeueDiff < requeue_delay) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Sleeping for "  + (requeue_delay - requeueDiff)
                          + " ms due to requeue delay");
                }
                //incrementCounter(CounterType.RequeuesDelayed, ns);
                Thread.sleep(requeue_delay - requeueDiff);
              }

              // I &#9825 Google! It's so easy! No release necessary! Thread Safe!
              final double w = rate_limiter.acquire();
              cumulativeRateDelay.addAndGet(w);
              ev.processData(this, recvTime);
            }
            break;
          default:
              throw new IllegalStateException("Unknown consumer type: " + 
                  consumer_type + " for " + this);
          }
          errorCount = 0;
        } catch (RuntimeException e) {
          LOG.error("Exception in kafkaReader or Tsdb Writer ", e);
          incrementNamespaceCounter(CounterType.Exception, null);
          errorCount++;
          if (errorCount >= 5) {
            LOG.error("Too many errors, Killing the consumer thread " + this);
            throw e;
          }
        }
        nanoCtr = System.nanoTime();
      }
      
      LOG.warn("Consumer thread [" + this + "] has run out of messages");
    } catch (ConsumerRebalanceFailedException crfex) {
      LOG.error("Failed to read Kafka partition from the consumer "
          + "thread " + this, crfex);
      group.incrementRebalanceFailures();
    } catch (Exception e) {
      LOG.error("Unexpected exception in Kafka thread: " + this, e);
      //incrementCounter(CounterType.DroppedException, null);
    } catch (Throwable e) {
      LOG.error("Fatal exception in Kafka thread: " + this, e);
      System.exit(1);
    }

    shutdown();
    if (group.getRate() > 0) {
      LOG.error("Thread [" + this + "] has exited the consumer loop!");
    } else {
      LOG.info("Thread [" + this + "] has stopped consuming as the rate limit is zero");
    }
    thread_running.set(false);
  }
  
  /**
   * Attempts to gracefully shutdown the thread, closing the consumer and waiting
   * for the inflight RPCs to complete.
   */
  public void shutdown() {
    LOG.info("Shutting down thread [" + this + "]");
    try {
      if (consumer != null) {
        consumer.shutdown();
        consumer = null;
        LOG.info("Shutdown the kafka consumer on thread [" + this + "]");
      }
    } catch (Exception e) {
      LOG.error("Failed to shutdown the kafka consumer on thread [" + this + "]");
      if (group.getParent().getConfig()
              .getBoolean(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE 
          + "killOnShutdownError")) {
        LOG.error("Shutting down the entire process because of thread [" 
          + this + "]");
        System.exit(1);
      }
    } catch (Throwable e) {
      LOG.error("Fatal exception in Kafka thread: " + this, e);
      System.exit(1);
    }
  }
  
  /**
   * Increments a counter for the namespace in the map.
   * If the namespace is null or empty then we use the {@code MISSING_CLUSTER}.
   * If the counter hasn't been used yet, we create the mappings and store them.
   * @param type The type of counter to increment
   * @param namespace A namespace if given, null to use the MISSING_CLUSTER
   */
  public void incrementNamespaceCounter(final CounterType type, final String namespace) {
    incrementNamespaceCounter(type.toString(), namespace);
  }
  
  /**
   * Increments a counter for the namespace in the map.
   * If the namespace is null or empty then we use the {@code MISSING_CLUSTER}.
   * If the counter hasn't been used yet, we create the mappings and store them.
   * @param type The type of counter to increment
   * @param metric A namespace if given, null to use the MISSING_CLUSTER
   */
  public void incrementNamespaceCounter(final String type, final String metric) {
    Map<String, AtomicLong> namespaces = namespace_counters.get(type);
    if (namespaces == null) {
      namespaces = new ConcurrentHashMap<String, AtomicLong>();
      final Map<String, AtomicLong> existing = 
          namespace_counters.putIfAbsent(type, namespaces);
      if (existing != null) {
        // lost the race
        namespaces = existing;
      }
    }
    final String prefix = track_metric_prefix ? 
        getPrefix(metric) : DEFAULT_COUNTER_ID;
    AtomicLong counter = namespaces.get(prefix);
    if (counter == null) {
      counter = new AtomicLong();
      final AtomicLong existing = namespaces.put(prefix, counter);
      if (existing != null) {
        // lost the race
        counter = existing;
      }
    }
    counter.incrementAndGet();
  }

  /** @return the map of counters for this thread. Concurrent maps */
  public Map<String, Map<String, AtomicLong>> getNamespaceCounters() {
    return namespace_counters;
  }

  /** @return the number of messages received from Kafka */
  public long getMessagesReceived() {
    return messagesReceived.get();
  }

  /** @return the number of datapoints received from Kafka messages */
  public long getDatapointsReceived() {
    return datapointsReceived.get();
  }
  
  /** @return the number of deserialization errors */
  public long getDeserializationErrors() {
    return deserializationErrors.get();
  }
  
  /** @return the number of seconds spent waiting on the rate limiter */
  public double getCumulativeRateDelay() {
    return cumulativeRateDelay.get();
  }
  
  /** @return the number of ms spent waiting for kafka messages. */
  public double getKafkaWaitTime() {
    return kafkaWaitTime.get();
  }
  
  /** @return whether or not the thread is running */
  public boolean isThreadRunning() {
    return thread_running.get();
  }
  
  public TSDB getTSDB() {
    return tsdb;
  }
  
  @VisibleForTesting
  ConsumerConnector consumer() {
    return consumer;
  }
  
  @VisibleForTesting
  int threadID() {
    return thread_id;
  }

  @VisibleForTesting
  long requeueDelay() {
    return requeue_delay;
  }
  
  /**
   * Helper to pretty print the metric and tags for debugging
   * @param dp the data point
   * @return A string to log
   */
  String getDebugString(final IncomingDataPoint dp) {
    final StringBuilder buf = new StringBuilder()
      .append(dp.getMetric())
      .append(" ");
    
    for (Map.Entry<String, String> pair : dp.getTags().entrySet()) {
      buf.append(pair.getKey()).append("=").append(pair.getValue()).append(" ");
    }
    return buf.toString();
  }
  
  String getPrefix(final String metric) {
    if (Strings.isNullOrEmpty(metric)) {
      return DEFAULT_COUNTER_ID;
    }
    
    int idx = metric.indexOf(".");
    if (idx < 1) {
      return DEFAULT_COUNTER_ID;
    }
    return metric.substring(0, idx);
  }
}
