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
