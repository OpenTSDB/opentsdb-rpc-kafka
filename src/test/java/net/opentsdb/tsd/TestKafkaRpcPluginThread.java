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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import net.opentsdb.core.HistogramCodecManager;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.Aggregate;
import net.opentsdb.data.Histogram;
import net.opentsdb.data.Metric;
import net.opentsdb.data.TypedIncomingData;
import net.opentsdb.data.deserializers.Deserializer;
import net.opentsdb.data.deserializers.JSONDeserializer;
import net.opentsdb.tsd.KafkaRpcPluginGroup.TsdbConsumerType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;

import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.stumbleupon.async.Deferred;

@PowerMockIgnore({ "javax.management.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, KafkaRpcPluginThread.class, Config.class,
  Thread.class,
 Consumer.class, ConsumerConfig.class, PleaseThrottleException.class})
public class TestKafkaRpcPluginThread {
  private static final String METRIC = "sys.cpu.user";
  private static final String METRIC2 = "sys.cpu.iowait";
  private static final String PREFIX = "sys";
  private static final long TS = 1492641000L;
  private static final String LOCALHOST = "localhost";
  private static final String TOPICS = "TSDB_600_1,TSDB_600_2,TSDB_600_3";
  private static final String GROUPID = "testGroup";
  private static final String ZKS = "192.168.1.1:2181";
  private Map<String, String> TAGS = ImmutableMap.<String, String>builder()
    .put("host", "web01")
    .build();
  
  private TSDB tsdb;
  private KafkaRpcPluginConfig config;
  private KafkaRpcPluginGroup group;
  private ConsumerConnector consumer_connector;
  private ConsumerIterator<byte[], byte[]> iterator;
  private List<KafkaStream<byte[], byte[]>> stream_list;
  private MessageAndMetadata<byte[], byte[]> message;
  private RateLimiter rate_limiter;
  private TypedIncomingData data;
  private KafkaRpcPlugin parent;
  private KafkaStorageExceptionHandler requeue;
  private ConcurrentMap<String, Map<String, AtomicLong>> counters;
  private Deserializer deserializer;

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    config = new KafkaRpcPluginConfig(new Config(false));
    group = mock(KafkaRpcPluginGroup.class);
    message = mock(MessageAndMetadata.class);
    rate_limiter = mock(RateLimiter.class);
    requeue = mock(KafkaStorageExceptionHandler.class);
    counters = new ConcurrentHashMap<String, Map<String, AtomicLong>>();
    deserializer = new JSONDeserializer();
    
    consumer_connector = mock(ConsumerConnector.class);

    mockStatic(Consumer.class);
    when(Consumer.createJavaConsumerConnector((ConsumerConfig) any()))
            .thenReturn(consumer_connector);
    
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getStorageExceptionHandler()).thenReturn(requeue);
    
    parent = mock(KafkaRpcPlugin.class);
    when(parent.getHost()).thenReturn(LOCALHOST);
    when(parent.getTSDB()).thenReturn(tsdb);
    when(parent.getConfig()).thenReturn(config);
    when(parent.getNamespaceCounters()).thenReturn(counters);
    when(parent.trackMetricPrefix()).thenReturn(true);
    
    when(group.getParent()).thenReturn(parent);
    when(group.getRateLimiter()).thenReturn(rate_limiter);
    when(group.getGroupID()).thenReturn(GROUPID);
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.RAW);
    when(group.getDeserializer()).thenReturn(deserializer);
    
    config.overrideConfig(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX 
        + "zookeeper.connect", ZKS);
    
    stream_list = mock(List.class);
    when(consumer_connector.createMessageStreamsByFilter(
        (TopicFilter) any(), anyInt())).thenReturn(stream_list);

    final KafkaStream<byte[], byte[]> stream = mock(KafkaStream.class);
    when(stream_list.get(0)).thenReturn(stream);

    iterator = mock(ConsumerIterator.class);
    when(stream.iterator()).thenReturn(iterator);

    when(iterator.hasNext()).thenReturn(true).thenReturn(false);
    when(iterator.next()).thenReturn(message);
    
    PowerMockito.mockStatic(ConsumerConfig.class);
    PowerMockito.whenNew(ConsumerConfig.class).withAnyArguments()
      .thenReturn(mock(ConsumerConfig.class));
    
    PowerMockito.mockStatic(Consumer.class);
    when(Consumer.createJavaConsumerConnector(any(ConsumerConfig.class)))
      .thenReturn(consumer_connector);
  }

  @Test
  public void ctor() throws Exception {
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
    assertEquals(1, writer.threadID());
    assertEquals(GROUPID + "_1_" + LOCALHOST, writer.toString());
    assertNull(writer.consumer());
    assertEquals(0, writer.requeueDelay());
  }

  @Test(expected = NullPointerException.class)
  public void ctorNullGroup() throws Exception {
    new KafkaRpcPluginThread(null, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorNullTSDB() throws Exception {
    when(parent.getTSDB()).thenReturn(null);
      new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorNullRateLimiter() throws Exception {
    when(group.getRateLimiter()).thenReturn(null);
      new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorNullGroupID() throws Exception {
    when(group.getGroupID()).thenReturn(null);
      new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorEmptyGroupID() throws Exception {
    when(group.getGroupID()).thenReturn("");
      new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorNullHost() throws Exception {
    when(parent.getHost()).thenReturn(null);
      new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorEmptyHost() throws Exception {
    when(parent.getHost()).thenReturn("");
      new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorNullTopics() throws Exception {
    new KafkaRpcPluginThread(group, 1, null);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorEmptyTopics() throws Exception {
    new KafkaRpcPluginThread(group, 1, "");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void ctorNegativeThreadID() throws Exception {
    new KafkaRpcPluginThread(group, -1, TOPICS);
  }
    
  @Test
  public void ctorRequeue() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_RAW);
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
    assertEquals(writer.threadID(), 1);
    assertNull(writer.consumer());
    assertEquals(KafkaRpcPluginConfig.DEFAULT_REQUEUE_DELAY_MS, 
        writer.requeueDelay());
  }
  
  @Test
  public void ctorRequeueOverride() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + 
        "requeueDelay", "42");
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_RAW);
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
    assertEquals(writer.threadID(), 1);
    assertNull(writer.consumer());
    assertEquals(42, writer.requeueDelay());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorRequeueOverrideBadValue() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + 
        "requeueDelay", "notanumber");
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_RAW);
    new KafkaRpcPluginThread(group, 1, TOPICS);
  }
  
  @Test
  public void buildConsumerPropertiesDefaults() throws Exception {
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
    final Properties props = writer.buildConsumerProperties();
    assertEquals(GROUPID, props.get(KafkaRpcPluginThread.GROUP_ID));
    assertEquals(Integer.toString(1) + "_" + LOCALHOST, 
        props.get(KafkaRpcPluginThread.CONSUMER_ID));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_DEFAULT),
        props.get(KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_MS));
    assertEquals(KafkaRpcPluginConfig.AUTO_COMMIT_ENABLE_DEFAULT,
        props.get(KafkaRpcPluginConfig.AUTO_COMMIT_ENABLE));
    assertEquals(KafkaRpcPluginConfig.AUTO_OFFSET_RESET_DEFAULT,
        props.get(KafkaRpcPluginConfig.AUTO_OFFSET_RESET));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.REBALANCE_BACKOFF_MS_DEFAULT),
        props.get(KafkaRpcPluginConfig.REBALANCE_BACKOFF_MS));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.REBALANCE_RETRIES_DEFAULT),
        props.get(KafkaRpcPluginConfig.REBALANCE_RETRIES));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.ZK_SESSION_TIMEOUT_DEFAULT),
        props.get(KafkaRpcPluginConfig.ZOOKEEPER_SESSION_TIMEOUT_MS));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.ZK_CONNECTION_TIMEOUT_DEFAULT),
        props.get(KafkaRpcPluginConfig.ZOOKEEPER_CONNECTION_TIMEOUT_MS));
    assertEquals(ZKS, props.get("zookeeper.connect"));
  }
  
  @Test
  public void buildConsumerPropertiesGroupOverride() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX 
        + GROUPID + "." + "zookeeper.connect", "10.0.0.1:2181");
    // make sure a different group doesn't mess us up
    config.overrideConfig(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
        "diffGroup." + KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_MS, "1024");
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
    final Properties props = writer.buildConsumerProperties();
    assertEquals(GROUPID, props.get(KafkaRpcPluginThread.GROUP_ID));
    assertEquals(Integer.toString(1) + "_" + LOCALHOST, 
        props.get(KafkaRpcPluginThread.CONSUMER_ID));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_DEFAULT),
        props.get(KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_MS));
    assertEquals(KafkaRpcPluginConfig.AUTO_COMMIT_ENABLE_DEFAULT,
        props.get(KafkaRpcPluginConfig.AUTO_COMMIT_ENABLE));
    assertEquals(KafkaRpcPluginConfig.AUTO_OFFSET_RESET_DEFAULT,
        props.get(KafkaRpcPluginConfig.AUTO_OFFSET_RESET));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.REBALANCE_BACKOFF_MS_DEFAULT),
        props.get(KafkaRpcPluginConfig.REBALANCE_BACKOFF_MS));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.REBALANCE_RETRIES_DEFAULT),
        props.get(KafkaRpcPluginConfig.REBALANCE_RETRIES));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.ZK_SESSION_TIMEOUT_DEFAULT),
        props.get(KafkaRpcPluginConfig.ZOOKEEPER_SESSION_TIMEOUT_MS));
    assertEquals(
        Integer.toString(KafkaRpcPluginConfig.ZK_CONNECTION_TIMEOUT_DEFAULT),
        props.get(KafkaRpcPluginConfig.ZOOKEEPER_CONNECTION_TIMEOUT_MS));
    assertEquals("10.0.0.1:2181", props.get("zookeeper.connect"));
  }
    
  @Test
  public void buildConsumerConnector() throws Exception {
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
   assertNotNull(writer.buildConsumerConnector());
  }
  
  @Test
  public void shutdown() throws Exception {
    final KafkaRpcPluginThread writer = 
        new KafkaRpcPluginThread(group, 1, TOPICS);
    writer.run();
    writer.shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }
    
  @Test
  public void runNoData() throws Exception {
    when(iterator.hasNext()).thenReturn(false);

    final KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(tsdb, never()).addHistogramPoint(anyString(), anyLong(), 
        any(byte[].class), anyMap());
    verify(tsdb, never()).addAggregatePoint(anyString(), anyLong(), anyLong(), 
        anyMap(), anyBoolean(), anyString(), anyString(), anyString());
    verify(consumer_connector, times(1))
      .createMessageStreamsByFilter(any(TopicFilter.class), anyInt());
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }
  
  @Test
  public void runNoDataRestart() throws Exception {
    when(iterator.hasNext()).thenReturn(false);

    final KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    writer.run();
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(tsdb, never()).addHistogramPoint(anyString(), anyLong(), 
        any(byte[].class), anyMap());
    verify(tsdb, never()).addAggregatePoint(anyString(), anyLong(), anyLong(), 
        anyMap(), anyBoolean(), anyString(), anyString(), anyString());
    verify(consumer_connector, times(2))
      .createMessageStreamsByFilter(any(TopicFilter.class), anyInt());
    verify(writer, times(2)).shutdown();
    verify(consumer_connector, times(2)).shutdown();
  }

  @Test
  public void runNoStreams() throws Exception {
    when(stream_list.get(0))
            .thenThrow(new ArrayIndexOutOfBoundsException());

    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(tsdb, never()).addHistogramPoint(anyString(), anyLong(), 
        any(byte[].class), anyMap());
    verify(tsdb, never()).addAggregatePoint(anyString(), anyLong(), anyLong(), 
        anyMap(), anyBoolean(), anyString(), anyString(), anyString());
    verify(consumer_connector, times(1))
      .createMessageStreamsByFilter(any(TopicFilter.class), anyInt());
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }

  @Test
  public void runGoodMessageRaw() throws Exception {
    setupRawData(false);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRawCounter", "storedRawCounter" });
  }

  @Test
  public void runGoodMessageRawList() throws Exception {
    setupRawDataList(false);
    KafkaRpcPluginThread writer = Mockito.spy(
            new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verify(tsdb, times(1)).addPoint(METRIC2, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRawCounter", "storedRawCounter" }, 2);
  }

  @Test
  public void runGoodMessageRollup() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.ROLLUP);
    setupAggData(false, false);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addAggregatePoint(METRIC, TS, 42L, TAGS, false, 
        "1h", "sum", null);
    verifyCtrsInc(new String[]{ "readRollupCounter", "storedRollupCounter" });
  }
  
  @Test
  public void runGoodMessagePreAgg() throws Exception {
    setupAggData(false, true);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addAggregatePoint(METRIC, TS, 42L, TAGS, true, 
        null, null, "sum");
    verifyCtrsInc(new String[]{ "readAggregateCounter", 
        "storedAggregateCounter" });
  }
  
  @Test
  public void runGoodMessageHistogram() throws Exception {
    setupHistogramData(false);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addHistogramPoint(eq(METRIC), eq(TS), 
        any(byte[].class), eq(TAGS));
    verifyCtrsInc(new String[]{ "readHistogramCounter", 
        "storedHistogramCounter" });
  }
  
  @Test
  public void runGoodMessageRequeueRaw() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_RAW);
    setupRawData(true);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRequeueRawCounter",
        "storedRequeueRawCounter" });
  }

  @Test
  public void runGoodMessageRequeueRawList() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_RAW);
    setupRawDataList(true);
    KafkaRpcPluginThread writer = Mockito.spy(
            new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verify(tsdb, times(1)).addPoint(METRIC2, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRequeueRawCounter", "storedRequeueRawCounter" }, 2);
  }
  
  @Test
  public void runGoodMessageRequeueRollup() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_ROLLUP);
    setupAggData(true, false);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addAggregatePoint(METRIC, TS, 42L, TAGS, false, 
        "1h", "sum", null);
    verifyCtrsInc(new String[]{ "readRequeueRollupCounter", 
      "storedRequeueRollupCounter" });
  }
  
  @Test
  public void runGoodMessageRequeuePreAgg() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.REQUEUE_ROLLUP);
    setupAggData(true, true);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addAggregatePoint(METRIC, TS, 42L, TAGS, true, 
        null, null, "sum");
    verifyCtrsInc(new String[]{ "readRequeueAggregateCounter", 
        "storedRequeueAggregateCounter" });
  }
  
  @Test
  public void runGoodMessageRequeueHistogram() throws Exception {
    setupHistogramData(true);
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addHistogramPoint(eq(METRIC), eq(TS), 
        any(byte[].class), eq(TAGS));
    verifyCtrsInc(new String[]{ "readRequeueHistogramCounter", 
        "storedRequeueHistogramCounter" });
  }

  @Test
  public void runEmptyData() throws Exception {
    when(message.message()).thenReturn(new byte[] { '{', '}' });
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(tsdb, never()).addHistogramPoint(anyString(), anyLong(), 
        any(byte[].class), anyMap());
    verify(tsdb, never()).addAggregatePoint(anyString(), anyLong(), anyLong(), 
        anyMap(), anyBoolean(), anyString(), anyString(), anyString());
    verifyMessageRead(writer, false);
  }

  @Test
  public void runEmptyMetric() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    data = new Metric(null, TS, "42", TAGS);
    when(message.message()).thenReturn(JSON.serializeToBytes(data));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));

    writer.run();
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(tsdb, never()).addHistogramPoint(anyString(), anyLong(), 
        any(byte[].class), anyMap());
    verify(tsdb, never()).addAggregatePoint(anyString(), anyLong(), anyLong(), 
        anyMap(), anyBoolean(), anyString(), anyString(), anyString());
    verifyMessageRead(writer, false);
  }
  
  @Test
  public void runStorageFailure() throws Exception {
    setupRawData(false);
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(mock(HBaseException.class)));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, true);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRawCounter", "requeuedRawCounter",
        "storageExceptionCounter" });
  }

  @Test
  public void runStorageFailureRollup() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.ROLLUP);
    setupAggData(false, false);
    when(tsdb.addAggregatePoint(anyString(), anyLong(), anyLong(), anyMap(), 
        anyBoolean(), anyString(), anyString(), anyString()))
      .thenReturn(Deferred.fromResult(mock(HBaseException.class)));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, true);
    verify(tsdb, times(1)).addAggregatePoint(METRIC, TS, 42L, TAGS, false, 
        "1h", "sum", null);
    verifyCtrsInc(new String[]{ "readRollupCounter", "requeuedRollupCounter",
      "storageExceptionCounter" });
  }
  
  @Test
  public void runStorageFailurePreAgg() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.ROLLUP);
    setupAggData(false, true);
    when(tsdb.addAggregatePoint(anyString(), anyLong(), anyLong(), anyMap(), 
        anyBoolean(), anyString(), anyString(), anyString()))
      .thenReturn(Deferred.fromResult(mock(HBaseException.class)));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, true);
    verify(tsdb, times(1)).addAggregatePoint(METRIC, TS, 42L, TAGS, true, 
        null, null, "sum");
    verifyCtrsInc(new String[]{ "readAggregateCounter", "requeuedAggregateCounter",
      "storageExceptionCounter" });
  }
  
  @Test
  public void runStorageFailureHistogram() throws Exception {
    when(group.getConsumerType()).thenReturn(TsdbConsumerType.ROLLUP);
    setupHistogramData(false);
    when(tsdb.addHistogramPoint(anyString(), anyLong(), any(byte[].class), 
        anyMap()))
      .thenReturn(Deferred.fromResult(mock(HBaseException.class)));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, true);
    verify(tsdb, times(1)).addHistogramPoint(eq(METRIC), eq(TS), 
        any(byte[].class), eq(TAGS));
    verifyCtrsInc(new String[]{ "readHistogramCounter", "requeuedHistogramCounter",
      "storageExceptionCounter" });
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void runRequeueWTF() throws Exception {
    setupRawData(false);
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenThrow(new RuntimeException("Boo!"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, false);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRawCounter", "exceptionCounter" });
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void runRequeueThrottling() throws Exception {
    setupRawData(false);
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(mock(PleaseThrottleException.class)));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();

    verifyMessageRead(writer, true);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verifyCtrsInc(new String[]{ "readRawCounter", "requeuedRawCounter",
      "pleaseThrottleExceptionCounter" });
  }
  
  @Test
  public void runTooManyRuntimeException() throws Exception {
    when(iterator.hasNext()).thenReturn(true);
    when(iterator.next()).thenThrow(new RuntimeException("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }

  @Test
  public void runIteratorHasNextRuntimeException() throws Exception {
    when(iterator.hasNext()).thenThrow(new RuntimeException("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }

  @Test(expected = Exception.class)
  public void runIteratorHasNextException() throws Exception {
    when(iterator.hasNext()).thenThrow(new Exception("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
  }

  @Test
  public void runIteratorNextRuntimeException() throws Exception {
    when(iterator.next()).thenThrow(new RuntimeException("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }

  @Test(expected = Exception.class)
  public void runIteratorNextException() throws Exception {
    when(iterator.next()).thenThrow(new Exception("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
  }

  @Test
  public void runConsumerRuntimeException() throws Exception {
    when(consumer_connector.createMessageStreamsByFilter(
        (TopicFilter) any(), anyInt())).thenThrow(
            new RuntimeException("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }

  @Test(expected = Exception.class)
  public void runConsumerException() throws Exception {
    when(consumer_connector.createMessageStreamsByFilter(
        (TopicFilter) any(), anyInt())).thenThrow(
            new Exception("Foobar"));
    KafkaRpcPluginThread writer = Mockito.spy(
        new KafkaRpcPluginThread(group, 1, TOPICS));
    writer.run();
    
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
  }
  
  // ------ HELPERS ---------
  
  private void setupRawData(final boolean requeued) {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    data = new Metric(METRIC, TS, "42", TAGS);
    if (requeued) {
      data.setRequeueTS(TS + 60);
    }
    when(message.message()).thenReturn(JSON.serializeToBytes(data));
  }

  private void setupRawDataList(final boolean requeued) {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
            .thenReturn(Deferred.fromResult(null));

    TypedIncomingData ev1 = new Metric(METRIC, TS, "42", TAGS);
    TypedIncomingData ev2 = new Metric(METRIC2, TS, "42", TAGS);

    if (requeued) {
      ev1.setRequeueTS(TS + 60);
      ev2.setRequeueTS(TS + 60);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(new String(JSON.serializeToBytes(ev1)));
    sb.append(",");
    sb.append(new String(JSON.serializeToBytes(ev2)));
    sb.append("]");

    when(message.message()).thenReturn(sb.toString().getBytes());
  }

  private void setupAggData(final boolean requeued, final boolean is_group_by) {
    when(tsdb.addAggregatePoint(anyString(), anyLong(), anyLong(), anyMap(), 
      anyBoolean(), anyString(), anyString(), anyString()))
      .thenReturn(Deferred.fromResult(null));
    if (is_group_by) {
      data = new Aggregate(METRIC, TS, "42", TAGS, null, null, "sum");
    } else {
      data = new Aggregate(METRIC, TS, "42", TAGS, "1h", "sum", null);
    }
    if (requeued) {
      data.setRequeueTS(TS + 60);
    }
    when(message.message()).thenReturn(JSON.serializeToBytes(data));
  }

  private void setupHistogramData(final boolean requeued) {
    config.overrideConfig("tsd.core.histograms.config", 
        "{\"net.opentsdb.core.SimpleHistogramDecoder\": 42}");
    final HistogramCodecManager manager;
    manager = new HistogramCodecManager(tsdb);
    when(tsdb.histogramManager()).thenReturn(manager);
    when(tsdb.addHistogramPoint(anyString(), anyLong(), any(byte[].class), 
        anyMap()))
      .thenReturn(Deferred.fromResult(null));
    final Histogram histo = new Histogram();
    histo.setMetric(METRIC);
    histo.setTimestamp(TS);
    histo.setTags(new HashMap<String, String>(TAGS));
    histo.setOverflow(1);
    histo.setBuckets(ImmutableMap.<String, Long>builder()
        .put("0,1", 42L)
        .put("1,5", 24L)
        .build());
    data = histo;
    if (requeued) {
      data.setRequeueTS(TS + 60);
    }
    when(message.message()).thenReturn(JSON.serializeToBytes(data));
  }

  /**
   * Runs through the list of types for the namespace and expects them to
   * be 1. Any other CounterTypes should be zero. Uses default test NAMESPACE
   * @param types The types to look for
   */
  private void verifyCtrsInc(final String[] types) {
    verifyCtrsInc(types, PREFIX, 1);
  }

  /**
   * Runs through the list of types for the namespace and expects them to
   * be 1. Any other CounterTypes should be zero. Uses default test NAMESPACE
   * @param types The types to look for
   * @param expectedCount Expected count (to be used for list message processing)
   */
  private void verifyCtrsInc(final String[] types, Integer expectedCount) {
    verifyCtrsInc(types, PREFIX, expectedCount);
  }
    
  /**
   * Runs through the list of types for the namespace and expects them to
   * be 1. Any other CounterTypes should be zero.
   * @param types The types to look for
   * @param namespace A namespace to look for
   */
  private void verifyCtrsInc(final String[] types, final String namespace) {
    verifyCtrsInc(types, namespace, 1);
  }

  /**
   * Runs through the list of types for the namespace and expects them to
   * be 1. Any other CounterTypes should be zero.
   * @param types The types to look for
   * @param namespace A namespace to look for
   * @param expectedCount Expected count (to be used for list message processing)
   */
  private void verifyCtrsInc(final String[] types, final String namespace, final Integer expectedCount) {
    for (final String type : types) {
      if (counters.get(type) == null) {
        throw new AssertionError("No ns map for type [" + type + "]");
      }
      if (counters.get(type).get(namespace) == null) {
        throw new AssertionError("No counter for type [" + type + 
            "] and ns [" + namespace + "]");
      }
      if (counters.get(type).get(namespace).get() != expectedCount) {
        throw new AssertionError("Counter was not zero for [" + type + 
            "] and ns [" + namespace + "]");
      }
    }
    verifyCtrsZero(types, namespace);
  }
    
  /**
   * Runs through the whole counter map and fails on any that are not in the
   * skip list.
   * @param types The types to skip
   * @param namespace A namespace to look for
   */
  private void verifyCtrsZero(final String[] types, final String namespace) {
    for (final Entry<String, Map<String, AtomicLong>> type_map : 
        counters.entrySet()) {
      boolean unexpected_type = true;
      for (final String type : types) {
        if (type_map.getKey().equals(type)) {
          for (final Entry<String, AtomicLong> ns : type_map.getValue()
              .entrySet()) {
            if (!ns.getKey().equals(namespace)) {
              throw new AssertionError("Found a non-zero counter for type [" + 
                  type + "] and ns [" + ns.getKey() + "]");  
            }
          }
          unexpected_type = false;
        }
      }
      
      if (unexpected_type) {
        throw new AssertionError("Found a non-zero counter for type [" + 
            type_map.getKey() + "]");
      }
    }
  }
  
  private void verifyMessageRead(final KafkaRpcPluginThread writer,
                                 final boolean requeued) {
    verify(writer, times(1)).shutdown();
    verify(consumer_connector, times(1)).shutdown();
    verify(consumer_connector, times(1))
      .createMessageStreamsByFilter(any(TopicFilter.class), anyInt());
    verify(iterator, times(2)).hasNext();
    if (requeued) {
      verify(requeue, times(1)).handleError( 
          any(IncomingDataPoint.class), any(Exception.class));
    } else {
      verify(requeue, never()).handleError(
          any(IncomingDataPoint.class), any(Exception.class));
    }
    if (data != null) {
      verify(rate_limiter, times(1)).acquire();
    }
  }
}
