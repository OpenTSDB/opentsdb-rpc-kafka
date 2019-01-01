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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.CallQueueTooBigException;
import org.hbase.async.HBaseException;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.core.Tags;
import net.opentsdb.tsd.KafkaRpcPluginThread;
import net.opentsdb.tsd.KafkaRpcPluginThread.CounterType;
import net.opentsdb.uid.FailedToAssignUniqueIdException;

public class Metric extends IncomingDataPoint implements TypedIncomingData {
  private static final Logger LOG = LoggerFactory.getLogger(Metric.class);
  
  /** The time, in ms, when this data point was sent to Kafka for requeueing */
  private long requeue_ts;
  
  public Metric() {
    
  }
  
  public Metric(final String metric, 
                final long timestamp, 
                final String value,
                final Map<String, String> tags) {
    try {
      // Because 2.3 still has the fields private we can't set them directly
      // thus we do the happy fun disgustingly ugly and slow reflection dance.
      Field temp = IncomingDataPoint.class.getDeclaredField("metric");
      temp.setAccessible(true);
      temp.set(this, metric);
      temp.setAccessible(false);
      
      temp = IncomingDataPoint.class.getDeclaredField("timestamp");
      temp.setAccessible(true);
      temp.set(this, timestamp);
      temp.setAccessible(false);
      
      temp = IncomingDataPoint.class.getDeclaredField("value");
      temp.setAccessible(true);
      temp.set(this, value);
      temp.setAccessible(false);
      
      temp = IncomingDataPoint.class.getDeclaredField("tags");
      temp.setAccessible(true);
      temp.set(this, tags);
      temp.setAccessible(false);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException("WTF? The IncomingDataPoints class was "
          + "missing a field.", e);
    } catch (SecurityException e) {
      throw new IllegalStateException("WTF? Security issue with the "
          + "IncomingDataPoints class.", e);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("WTF? Failed to set a value on the "
          + "IncomingDataPoints class.", e);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException("WTF? Security issue with the "
          + "IncomingDataPoints class.", e);
    }
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
      consumer.incrementNamespaceCounter(CounterType.ReadRequeuedRaw, getMetric());
    } else {
      consumer.incrementNamespaceCounter(CounterType.ReadRaw, getMetric());
    }
    
    class SuccessCB implements Callback<Object, Object> {

      @Override
      public Object call(final Object ignored) throws Exception {
        if (requeue_ts > 0) {
          consumer.incrementNamespaceCounter(CounterType.StoredRequeuedRaw, getMetric());
        } else {
          consumer.incrementNamespaceCounter(CounterType.StoredRaw, getMetric());
        }
        return null;
      }
      
    }
    
    class ErrCB implements Callback<Object, Exception> {

      @Override
      public Object call(final Exception ex) throws Exception {
        if (LOG.isDebugEnabled()) {
          if (ex instanceof HBaseException) {
            LOG.debug("Requeing data point [" + Metric.this + "] due to error: " 
                + ex.getMessage());
          } else {
            LOG.debug("Requeing data point [" + Metric.this + "] due to error", ex);
          }
        }
        if (consumer.getTSDB().getStorageExceptionHandler() != null) {
          consumer.getTSDB().getStorageExceptionHandler()
            .handleError(Metric.this, ex);
          consumer.incrementNamespaceCounter(CounterType.RequeuedRaw, getMetric());
        }
        
        if (ex instanceof PleaseThrottleException) {
          consumer.incrementNamespaceCounter(CounterType.PleaseThrottle, getMetric());
        } else if (ex instanceof CallQueueTooBigException) {
          consumer.incrementNamespaceCounter(CounterType.PleaseThrottle, getMetric());
        } else if (ex instanceof HBaseException) {
          consumer.incrementNamespaceCounter(CounterType.StorageException, getMetric());
        } else if (ex instanceof FailedToAssignUniqueIdException) {
          consumer.incrementNamespaceCounter(CounterType.UIDAbuse, getMetric());
//        } else if (ex instanceof BlacklistedMetricException) {
//          consumer.incrementCounter(blacklisted_ctr_name, metric);
        } else if (ex.getCause() != null && 
            ex.getCause() instanceof FailedToAssignUniqueIdException) {
          consumer.incrementNamespaceCounter(CounterType.UIDAbuse, getMetric());
        } else {
          consumer.incrementNamespaceCounter(CounterType.UnknownException, getMetric());
        }
        
        return null;
      }
    }
    
    if (!validate(null)) {
      consumer.incrementNamespaceCounter(CounterType.IllegalArgument, getMetric());
      return;
    }
    
    try {
      if (Tags.looksLikeInteger(getValue())) {
        consumer.getTSDB().addPoint(getMetric(), getTimestamp(), Long.parseLong(getValue()), getTags())
          .addCallback(new SuccessCB())
          .addErrback(new ErrCB());
      } else if (fitsInFloat(getValue())) {
        consumer.getTSDB().addPoint(getMetric(), getTimestamp(), Float.parseFloat(getValue()), getTags())
          .addCallback(new SuccessCB())
          .addErrback(new ErrCB());
      } else {
        consumer.getTSDB().addPoint(getMetric(), getTimestamp(), Double.parseDouble(getValue()), getTags())
          .addCallback(new SuccessCB())
          .addErrback(new ErrCB());
      }
    } catch (IllegalArgumentException e) {
      consumer.incrementNamespaceCounter(CounterType.IllegalArgument, getMetric());
      return;
    } catch (Exception e) {
      LOG.error("Unexpected exception adding data point " + this, e);
      consumer.incrementNamespaceCounter(CounterType.Exception, getMetric());
    }
  }
  
  /**
   * Pre-validation of the various fields to make sure they're valid
   * @param details a map to hold detailed error message. If null then 
   * the errors will be logged
   * @return true if data point is valid, otherwise false
   */
  public boolean validate(final List<Map<String, Object>> details) {
    if (this.getMetric() == null || this.getMetric().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Metric name was empty"));
      }
      LOG.warn("Metric name was empty: " + this);
      return false;
    }

    //TODO add blacklisted metric validatin here too
    
    if (this.getTimestamp() <= 0) {
      if (details != null) {
        details.add(getHttpDetails("Invalid timestamp"));
      }
      LOG.warn("Invalid timestamp: " + this);
      return false;
    }

    if (this.getValue() == null || this.getValue().isEmpty()) {
      if (details != null) {
        details.add(getHttpDetails("Empty value"));
      }
      LOG.warn("Empty value: " + this);
      return false;
    }

    if (this.getTags() == null || this.getTags().size() < 1) {
      if (details != null) {
        details.add(getHttpDetails("Missing tags"));
      }
      LOG.warn("Missing tags: " + this);
      return false;
    }
    return true;
  }
  
  /**
   * Creates a map with an error message and this data point to return
   * to the HTTP put data point RPC handler
   * @param message The message to log
   * @return A map to append to the HTTP response
   */
  protected final Map<String, Object> getHttpDetails(final String message) {
    final Map<String, Object> map = new HashMap<String, Object>();
    map.put("error", message);
    map.put("datapoint", this);
    return map;
  }
  
  /**
   * Returns true if the given string can fit into a float.
   * @param value The String holding the float value.
   * @return true if the value can fit into a float, false otherwise.
   * @throws NumberFormatException if the value is not numeric.
   */
  public static boolean fitsInFloat(final String value) {
    // TODO - probably still a better way to do this and we could save a lot
    // of space by dropping useless precision, but for now this should help. 
    final double d = Double.parseDouble(value);
    return ((float) d) == d;
  }
}
