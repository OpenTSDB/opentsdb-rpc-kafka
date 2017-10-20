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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.KafkaRpcPluginGroup.TsdbConsumerType;
import net.opentsdb.utils.Config;

import org.jboss.netty.util.HashedWheelTimer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PowerMockIgnore({ "javax.management.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, Config.class, KafkaRpcPluginThread.class,
  KafkaRpcPluginGroup.class, ExecutorService.class, KafkaRpcPlugin.class })
public class TestKafkaRpcPluginGroup {
  private static final String LOCALHOST = "localhost";
  private static final String GROUPID = "testGroup";
  private static final String CONF_PREFIX = 
      KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + GROUPID;
  
  private KafkaRpcPlugin parent;
  private KafkaRpcPluginConfig config;
  private TSDB tsdb;
  private HashedWheelTimer timer;
  private List<KafkaRpcPluginThread> threads;
  private ExecutorService pool;
  private KafkaStorageExceptionHandler requeue;
  
  @Before
  public void setup() throws Exception {
    config = new KafkaRpcPluginConfig(new Config(false));
    tsdb = PowerMockito.mock(TSDB.class);
    timer = mock(HashedWheelTimer.class);
    parent = mock(KafkaRpcPlugin.class);
    
    config.overrideConfig(CONF_PREFIX + ".topics", 
        "TSDB_600_1,TSDB_600_2,TSDB_600_3");
    config.overrideConfig(CONF_PREFIX + ".consumerType", "raw");
    threads = new ArrayList<KafkaRpcPluginThread>(1);
    PowerMockito.whenNew(KafkaRpcPluginThread.class).withAnyArguments()
      .thenAnswer(new Answer<KafkaRpcPluginThread>() {
        @Override
        public KafkaRpcPluginThread answer(InvocationOnMock invocation)
            throws Throwable {
          final KafkaRpcPluginThread thread = 
              PowerMockito.mock(KafkaRpcPluginThread.class);
          when(thread.getState()).thenReturn(State.RUNNABLE);
          threads.add(thread);
          return thread;
        }
    });
    
    requeue = mock(KafkaStorageExceptionHandler.class);
    
    pool = mock(ExecutorService.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ((Thread)invocation.getArguments()[0]).start();
        return null;
      }
    }).when(pool).execute(any(Thread.class));
  
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getStorageExceptionHandler()).thenReturn(requeue);
    when(tsdb.getTimer()).thenReturn(timer);
    
    when(parent.getHost()).thenReturn(LOCALHOST);
    when(parent.getTSDB()).thenReturn(tsdb);
    when(parent.getConfig()).thenReturn(config);
    when(parent.getKafkaPool()).thenReturn(pool);
  }
  
  @Test
  public void ctorDefaults() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(threads.size(), KafkaRpcPluginConfig.DEFAULT_CONSUMER_THREADS);
    assertEquals(group.getRateLimiter().getRate(), 
        KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE, 0.001);
    assertEquals(group.getGroupID(), GROUPID);
    assertEquals(TsdbConsumerType.RAW, group.getConsumerType());
    verify(timer, times(1)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
    for (final KafkaRpcPluginThread thread : threads) {
      verify(thread, never()).start();
    }
  }
  
  @Test
  public void ctorRateOverride() throws Exception {
    config.overrideConfig(CONF_PREFIX + ".rate", "42");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(threads.size(), KafkaRpcPluginConfig.DEFAULT_CONSUMER_THREADS);
    assertEquals(group.getRateLimiter().getRate(), 42, 0.001);
    verify(timer, times(1)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
    for (final KafkaRpcPluginThread thread : threads) {
      verify(thread, never()).start();
    }
  }
  
  @Test
  public void ctorRateOverrideZero() throws Exception {
    config.overrideConfig(CONF_PREFIX + ".rate", "0");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(threads.size(), KafkaRpcPluginConfig.DEFAULT_CONSUMER_THREADS);
    assertEquals(group.getRateLimiter().getRate(), 
        KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE, 0.001);
    assertEquals(0, group.getRate(), 0.000);
    verify(timer, times(1)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
    for (final KafkaRpcPluginThread thread : threads) {
      verify(thread, never()).start();
    }
  }
  
  @Test
  public void ctorThreadOverride() throws Exception {
    config.overrideConfig(CONF_PREFIX + ".threads", "4");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(threads.size(), 4);
    assertEquals(group.getRateLimiter().getRate(), 
        KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE, 0.001);
    verify(timer, times(1)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
    for (final KafkaRpcPluginThread thread : threads) {
      verify(thread, never()).start();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullGroup() throws Exception {
    new KafkaRpcPluginGroup(parent, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyGroup() throws Exception {
    new KafkaRpcPluginGroup(parent, "");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMissingTopics() throws Exception {
    config.overrideConfig(CONF_PREFIX + ".topics", "");
    new KafkaRpcPluginGroup(parent, GROUPID);
  }

  @Test
  public void ctorTypeOverride() throws Exception {
    config.overrideConfig(CONF_PREFIX + ".consumerType", "requeue_raw");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(threads.size(), KafkaRpcPluginConfig.DEFAULT_CONSUMER_THREADS);
    assertEquals(TsdbConsumerType.REQUEUE_RAW, group.getConsumerType());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyTypeOverride() throws Exception {
    config.overrideConfig(CONF_PREFIX + ".consumerType", "");
    new KafkaRpcPluginGroup(parent, GROUPID);
  }
  
  @Test
  public void startThreads() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    group.start();
    for (final KafkaRpcPluginThread thread : threads) {
      verify(pool, times(1)).execute(thread);
    }
  }
  
  @Test
  public void startThreadsZeroRate() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + GROUPID 
        + ".rate", "0");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    group.start();
    for (final KafkaRpcPluginThread thread : threads) {
      verify(pool, never()).execute(thread);
    }
  }
  
  @Test
  public void monitorGoodState() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    when(threads.get(0).isThreadRunning()).thenReturn(true);
    when(threads.get(1).isThreadRunning()).thenReturn(true);
    group.run(null);
    for (final KafkaRpcPluginThread thread : threads) {
      verify(pool, never()).execute(thread);
    }
    verify(timer, times(2)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void monitorOneTerminated() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    when(threads.get(0).isThreadRunning()).thenReturn(true);
    when(threads.get(1).isThreadRunning()).thenReturn(false);
    group.run(null);
    verify(pool, never()).execute(threads.get(0));
    verify(pool, times(1)).execute(threads.get(1));
    verify(timer, times(2)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void monitorOneTerminatedRateZero() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + GROUPID 
        + ".rate", "0");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    when(threads.get(0).isThreadRunning()).thenReturn(false);
    when(threads.get(1).isThreadRunning()).thenReturn(false);
    group.run(null);
    verify(pool, never()).execute(threads.get(0));
    verify(pool, never()).execute(threads.get(1));
    verify(timer, times(2)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
  }
  
  @Test
  public void monitorException() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    when(threads.get(0).isThreadRunning()).thenThrow(new RuntimeException("Boo!"));
    when(threads.get(1).isThreadRunning()).thenReturn(true);
    group.run(null);
    verify(pool, never()).execute(threads.get(0));
    verify(pool, never()).execute(threads.get(1));
    verify(timer, times(2)).newTimeout(group, config.threadCheckInterval(), 
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void getRateDefault() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE, group.getRate(), 0.000);
  }
  
  @Test
  public void getRateOverride() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + GROUPID 
        + ".rate", "42");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(42, group.getRate(), 0.000);
  }
  
  @Test
  public void getRateOverrideZero() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + GROUPID 
        + ".rate", "0");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(0, group.getRate(), 0.000);
  }

  @Test
  public void incrementRebalanceFailures() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(0, group.getRebalanceFailures());
    group.incrementRebalanceFailures();
    assertEquals(1, group.getRebalanceFailures());
  }
  
  @Test
  public void metrics() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    assertEquals(0, group.getRestarts());
    assertEquals(0, group.getRebalanceFailures());
    assertEquals(0, group.getMessagesReceived());
    assertEquals(0, group.getDatapointsReceived());
    assertEquals(0, group.getDeserializationErrors());
  }

  @Test
  public void getNamespaceCounters() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    final Map<String, Map<String, Long>> counters = 
        new HashMap<String, Map<String, Long>>();
    group.getNamespaceCounters(counters);
    assertEquals(0, counters.size()); // nothing from the moc
  }
  
  @Test
  public void setRate() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    group.setRate(42);
    assertEquals(42, group.getRate(), 0.000);
    assertEquals(42, group.getRateLimiter().getRate(), 0.000);
    verify(threads.get(0), never()).shutdown();
    verify(threads.get(1), never()).shutdown();
  }
  
  @Test
  public void setRateZero() throws Exception {
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    group.setRate(0);
    assertEquals(0, group.getRate(), 0.000);
    // remains the default as we can't set the limiter to 0
    assertEquals(KafkaRpcPluginConfig.DEFAULT_CONSUMER_RATE, 
        group.getRateLimiter().getRate(), 0.000);
    verify(threads.get(0), times(1)).shutdown();
    verify(threads.get(1), times(1)).shutdown();
  }
  
  @Test
  public void setRateZeroToPositive() throws Exception {
    config.overrideConfig(KafkaRpcPluginConfig.PLUGIN_PROPERTY_BASE + GROUPID 
        + ".rate", "0");
    final KafkaRpcPluginGroup group = new KafkaRpcPluginGroup(parent, GROUPID);
    group.setRate(42);
    assertEquals(42, group.getRate(), 0.000);
    assertEquals(42, group.getRateLimiter().getRate(), 0.000);
    verify(threads.get(0), never()).shutdown();
    verify(threads.get(1), never()).shutdown();
  }
}
