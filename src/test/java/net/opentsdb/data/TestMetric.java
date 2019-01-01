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
package net.opentsdb.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyFloat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.KafkaRpcPluginThread;
import net.opentsdb.tsd.StorageExceptionHandler;
import net.opentsdb.tsd.KafkaRpcPluginThread.CounterType;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, PleaseThrottleException.class })
public class TestMetric {
  private static final String METRIC = "sys.cpu.user";
  private static final long TS = 1492641000L;
  private Map<String, String> TAGS = Maps.newHashMap(
      ImmutableMap.<String, String>builder()
      .put("host", "web01")
      .build());
  
  private TSDB tsdb;
  private KafkaRpcPluginThread consumer;
  private StorageExceptionHandler seh;
  
  @Before
  public void before() throws Exception {
    tsdb = PowerMockito.mock(TSDB.class);
    consumer = mock(KafkaRpcPluginThread.class);
    seh = mock(StorageExceptionHandler.class);
    
    when(consumer.getTSDB()).thenReturn(tsdb);
    when(tsdb.getStorageExceptionHandler()).thenReturn(seh);
  }
  
  @Test
  public void processDataSuccessfullLong() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    
    final Metric dp = new Metric(METRIC, TS, "42", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42L, TAGS);
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.StoredRaw, METRIC);
  }
  
  @Test
  public void processDataSuccessfullFloat() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyFloat(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    
    final Metric dp = new Metric(METRIC, TS, "42.5", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42.5F, TAGS);
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.StoredRaw, METRIC);
  }
  
  @Test
  public void processDataSuccessfullDouble() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyDouble(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    
    final Metric dp = new Metric(METRIC, TS, "42.1658413687", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 42.1658413687D, TAGS);
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.StoredRaw, METRIC);
  }
  
  @Test
  public void processDataSuccessfullZero() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    
    final Metric dp = new Metric(METRIC, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 0L, TAGS);
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.StoredRaw, METRIC);
  }
  
  @Test
  public void processDataNullValue() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    
    final Metric dp = new Metric(METRIC, TS, null, TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.IllegalArgument, METRIC);
  }
  
  @Test
  public void processDataNullMetric() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(null));
    
    final Metric dp = new Metric(null, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, never()).addPoint(anyString(), anyLong(), anyLong(), anyMap());
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.IllegalArgument, null);
  }
  
  @Test
  public void processDataThrottled() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(mock(PleaseThrottleException.class)));
    
    final Metric dp = new Metric(METRIC, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 0L, TAGS);
    verify(seh, times(1)).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.PleaseThrottle, METRIC);
  }
  
  @Test
  public void processDataHBaseException() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(mock(HBaseException.class)));
    
    final Metric dp = new Metric(METRIC, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 0L, TAGS);
    verify(seh, times(1)).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.StorageException, METRIC);
  }
  
  @Test
  public void processDataUnknownException() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenReturn(Deferred.fromResult(new RuntimeException()));
    
    final Metric dp = new Metric(METRIC, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 0L, TAGS);
    verify(seh, times(1)).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.UnknownException, METRIC);
  }
  
  @Test
  public void processDataIllegalArgument() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenThrow(new IllegalArgumentException());
    
    final Metric dp = new Metric(METRIC, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 0L, TAGS);
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.IllegalArgument, METRIC);
  }
  
  @Test
  public void processDataWTF() throws Exception {
    when(tsdb.addPoint(anyString(), anyLong(), anyLong(), anyMap()))
      .thenThrow(new RuntimeException());
    
    final Metric dp = new Metric(METRIC, TS, "0", TAGS);
    dp.processData(consumer, 0);
    verify(tsdb, times(1)).addPoint(METRIC, TS, 0L, TAGS);
    verify(seh, never()).handleError(eq(dp), any(Exception.class));
    verify(consumer, times(1)).incrementNamespaceCounter(CounterType.Exception, METRIC);
  }
}
