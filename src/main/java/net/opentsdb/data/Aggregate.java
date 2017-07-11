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
package net.opentsdb.data;

import java.util.Map;

import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;

import net.opentsdb.core.Tags;
import net.opentsdb.rollup.RollUpDataPoint;
import net.opentsdb.tsd.KafkaRpcPluginThread;
import net.opentsdb.tsd.KafkaRpcPluginThread.CounterType;
import net.opentsdb.uid.FailedToAssignUniqueIdException;

public class Aggregate extends RollUpDataPoint implements TypedIncomingData {
  private static final Logger LOG = LoggerFactory.getLogger(Aggregate.class);
  
  /** The time, in ms, when this data point was sent to Kafka for requeueing */
  private long requeue_ts;
  
  public Aggregate() {
    
  }
  
  public Aggregate(final String metric, 
                final long timestamp, 
                final String value,
                final Map<String, String> tags,
                final String interval,
                final String aggregator,
                final String group_aggregator) {
    this.metric = metric;
    this.timestamp = timestamp;
    this.value = value;
    this.tags = tags;
    setInterval(interval);
    setAggregator(aggregator);
    setGroupByAggregator(group_aggregator);
  }
  
  /** @return The requeue timestamp in milliseconds */
  public long getRequeueTS() {
    return requeue_ts;
  }
  
  /** @param requeue_ts The timestamp to set */
  public void setRequeueTS(final long requeue_ts) {
    this.requeue_ts = requeue_ts;
  }
  
  @Override
  public void processData(final KafkaRpcPluginThread consumer, 
                          final long receive_time) {
    if (Strings.isNullOrEmpty(getInterval())) {
      if (requeue_ts > 0) {
        consumer.incrementNamespaceCounter(CounterType.ReadRequeuedAggregate, metric);
      } else {
        consumer.incrementNamespaceCounter(CounterType.ReadAggregate, metric);
      }
    } else {
      if (requeue_ts > 0) {
        consumer.incrementNamespaceCounter(CounterType.ReadRequeuedRollup, metric);
      } else {
        consumer.incrementNamespaceCounter(CounterType.ReadRollup, metric);
      }
    }
    
    class SuccessCB implements Callback<Object, Object> {

      @Override
      public Object call(final Object ignored) throws Exception {
        if (Strings.isNullOrEmpty(getInterval())) {
          if (requeue_ts > 0) {
            consumer.incrementNamespaceCounter(CounterType.StoredRequeuedAggregate, metric);
          } else {
            consumer.incrementNamespaceCounter(CounterType.StoredAggregate, metric);
          }
        } else {
          if (requeue_ts > 0) {
            consumer.incrementNamespaceCounter(CounterType.StoredRequeuedRollup, metric);
          } else {
            consumer.incrementNamespaceCounter(CounterType.StoredRollup, metric);
          }
        }
        return null;
      }
      
    }
    
    class ErrCB implements Callback<Object, Exception> {

      @Override
      public Object call(final Exception ex) throws Exception {
        if (LOG.isDebugEnabled()) {
          if (ex instanceof HBaseException) {
            LOG.debug("Requeing data point [" + Aggregate.this + "] due to error: " 
                + ex.getMessage());
          } else {
            LOG.debug("Requeing data point [" + Aggregate.this + "] due to error", ex);
          }
        }
        if (consumer.getTSDB().getStorageExceptionHandler() != null) {
          consumer.getTSDB().getStorageExceptionHandler()
            .handleError(Aggregate.this, ex);
          if (Strings.isNullOrEmpty(getInterval())) {
            consumer.incrementNamespaceCounter(CounterType.RequeuedAggregate, metric);
          } else {
            consumer.incrementNamespaceCounter(CounterType.RequeuedRollup, metric);
          }
        }
        
        if (ex instanceof PleaseThrottleException) {
          consumer.incrementNamespaceCounter(CounterType.PleaseThrottle, metric);
        } else if (ex instanceof HBaseException) {
          consumer.incrementNamespaceCounter(CounterType.StorageException, metric);
        } else if (ex instanceof FailedToAssignUniqueIdException) {
          consumer.incrementNamespaceCounter(CounterType.UIDAbuse, metric);
//        } else if (ex instanceof BlacklistedMetricException) {
//          consumer.incrementCounter(blacklisted_ctr_name, metric);
        } else if (ex.getCause() != null && 
            ex.getCause() instanceof FailedToAssignUniqueIdException) {
          consumer.incrementNamespaceCounter(CounterType.UIDAbuse, metric);
        } else {
          consumer.incrementNamespaceCounter(CounterType.Exception, metric);
        }
        
        return null;
      }
    }
    
    if (!this.validate(null)) {
      consumer.incrementNamespaceCounter(CounterType.IllegalArgument, metric);
      return;
    }
    
    if (Tags.looksLikeInteger(value)) {
      consumer.getTSDB().addAggregatePoint(metric, 
                                           timestamp, 
                                           Long.parseLong(value), 
                                           tags, 
                                           getGroupByAggregator() != null, 
                                           getInterval(), 
                                           getAggregator(), 
                                           getGroupByAggregator())
        .addCallback(new SuccessCB())
        .addErrback(new ErrCB());
    } else if (Tags.fitsInFloat(value)) {
      consumer.getTSDB().addAggregatePoint(metric, 
                                           timestamp, 
                                           Float.parseFloat(value),
                                           tags, 
                                           getGroupByAggregator() != null, 
                                           getInterval(), 
                                           getAggregator(), 
                                           getGroupByAggregator())
        .addCallback(new SuccessCB())
        .addErrback(new ErrCB());
    } else {
      consumer.getTSDB().addAggregatePoint(metric, 
                                           timestamp, 
                                           Double.parseDouble(value),
                                           tags, 
                                           getGroupByAggregator() != null, 
                                           getInterval(), 
                                           getAggregator(), 
                                           getGroupByAggregator())
        .addCallback(new SuccessCB())
        .addErrback(new ErrCB());
    }
  }

}
