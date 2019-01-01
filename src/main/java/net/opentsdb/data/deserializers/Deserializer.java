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
package net.opentsdb.data.deserializers;

import java.util.List;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TypedIncomingData;
import net.opentsdb.tsd.KafkaRpcPluginThread;

/**
 * The interface used to describe a module that will deserialize raw bytes from
 * the Kafka buss.
 * <p>
 * Note that this class is implemented once per consumer group so make sure that
 * calls to {@link #deserialize(KafkaRpcPluginThread, byte[])} are thread safe.
 */
public interface Deserializer {

  /**
   * Called to initialize the implementation.
   * @param tsdb A non-null TSDB object to get the config object from.
   */
  public void initialize(final TSDB tsdb);
  
  /**
   * Called when the plugin is shut down to allow the deserializer to release 
   * any resources it requires.
   * @return Always return a non-null deferred that resolves to null or an
   * exception.
   */
  public Deferred<Object> shutdown();
  
  /**
   * Deserialize the data into a list of zero or more {@link TypedIncomingData} 
   * points. If deserialization failed, catch the exception, log it and return
   * null. Null data lists will increment the {@code deserializationErrors} 
   * counter.
   * @param consumer A non-null consumer the data cam from.
   * @param data A non-null data byte array.
   * @return A list of data points or null if deserialization failed.
   */
  public List<TypedIncomingData> deserialize(final KafkaRpcPluginThread consumer, 
                                             final byte[] data);
  
}
