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

import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Strings;
import com.stumbleupon.async.Callback;

import net.opentsdb.core.HistogramPojo;
import net.opentsdb.tsd.KafkaRpcPluginThread;
import net.opentsdb.tsd.KafkaRpcPluginThread.CounterType;
import net.opentsdb.uid.FailedToAssignUniqueIdException;

@JsonInclude(Include.NON_DEFAULT)
public class Histogram extends HistogramPojo implements TypedIncomingData {
  private static final Logger LOG = LoggerFactory.getLogger(Histogram.class);
  
  /** The time, in ms, when this data point was sent to Kafka for requeueing */
  private long requeue_ts;
  
  public Histogram() {
    
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
    if (requeue_ts > 0) {
      consumer.incrementNamespaceCounter(CounterType.ReadRequeuedHistogram, metric);
    } else {
      consumer.incrementNamespaceCounter(CounterType.ReadHistogram, metric);
    }
    
    class SuccessCB implements Callback<Object, Object> {

      @Override
      public Object call(final Object ignored) throws Exception {
        if (requeue_ts > 0) {
          consumer.incrementNamespaceCounter(CounterType.StoredRequeuedHistogram, metric);
        } else {
          consumer.incrementNamespaceCounter(CounterType.StoredHistogram, metric);
        }
        return null;
      }
      
    }
    
    class ErrCB implements Callback<Object, Exception> {

      @Override
      public Object call(final Exception ex) throws Exception {
        if (LOG.isDebugEnabled()) {
          if (ex instanceof HBaseException) {
            LOG.debug("Requeing data point [" + Histogram.this + "] due to error: " 
                + ex.getMessage());
          } else {
            LOG.debug("Requeing data point [" + Histogram.this + "] due to error", ex);
          }
        }
        if (consumer.getTSDB().getStorageExceptionHandler() != null) {
          consumer.getTSDB().getStorageExceptionHandler()
            .handleError(Histogram.this, ex);
          consumer.incrementNamespaceCounter(CounterType.RequeuedHistogram, metric);
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
    
    // validation and/or conversion before storage of histograms by 
    // decoding then re-encoding.
    final net.opentsdb.core.Histogram hdp;
    try {
      if (Strings.isNullOrEmpty(value)) {
        hdp = toSimpleHistogram(consumer.getTSDB());
      } else {
        hdp = consumer.getTSDB().histogramManager().decode(getId(), getBytes(), false);
      }
    } catch (Exception e) {
      consumer.incrementNamespaceCounter(CounterType.IllegalArgument, metric);
      return;
    }
    
    consumer.getTSDB().addHistogramPoint(metric, timestamp, 
        consumer.getTSDB().histogramManager().encode(hdp.getId(), hdp, true), 
        tags)
      .addCallback(new SuccessCB())
      .addErrback(new ErrCB());
  }

}
