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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.tsd.KafkaRpcPluginThread;

@JsonInclude(Include.NON_DEFAULT)
public class Metrics extends IncomingDataPoint implements TypedIncomingData {

  protected Map<String, String> metrics;
  
  /** the time, in ms, when this data point was sent to Kafka for requeuing */
  private long requeue_ts;
  
  /** @return the requeue timestamp in milliseconds */
  public long getRequeueTS() {
    return requeue_ts;
  }
  
  /** @param requeue_ts The timestamp to set */
  public void setRequeueTS(final long requeue_ts) {
    this.requeue_ts = requeue_ts;
  }
  
  public void setMetrics(final Map<String, String> metrics) {
    this.metrics = metrics;
  }
  
  public Map<String, String> getMetrics() {
    return metrics;
  }

  @Override
  public void processData(KafkaRpcPluginThread consumer, long receive_time) {
    // TODO Auto-generated method stub
    
  }
  
}
