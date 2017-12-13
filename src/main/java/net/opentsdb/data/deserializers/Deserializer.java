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
