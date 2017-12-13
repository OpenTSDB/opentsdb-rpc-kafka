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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.TypedIncomingData;
import net.opentsdb.tsd.KafkaRpcPluginThread;
import net.opentsdb.utils.JSON;

/**
 * The default JSON deserialization class for OpenTSDB messages.
 */
public class JSONDeserializer implements Deserializer {
  private static final Logger LOG = LoggerFactory.getLogger(JSONDeserializer.class);
  
  /** The type reference for a list of incoming data points. */
  private static final TypeReference<List<TypedIncomingData>> DATA_LIST = 
      new TypeReference<List<TypedIncomingData>>() {};
        
  @Override
  public void initialize(final TSDB tsdb) {
    // No-op
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }
  
  @Override
  public List<TypedIncomingData> deserialize(final KafkaRpcPluginThread consumer,
                                             final byte[] data) {
    if (data == null || data.length < 1) {
      LOG.error("Unable to deserialize data. Null or empty byte array.");
      return null;
    }
    
    final byte firstCharacter = data[0];
    List<TypedIncomingData> eventList;
    switch (firstCharacter) {
    case 91: // [
      try {
        eventList = JSON.parseToObject(data, DATA_LIST);
      } catch (Throwable ex1) {
        LOG.error("Unable to deserialize data ", ex1);
        return null;
      }
      break;
    case 123: // {
      try {
        eventList = new ArrayList<TypedIncomingData>(1);
        eventList.add(JSON.parseToObject(data, 
            TypedIncomingData.class));
      } catch (Throwable ex1) {
        LOG.error("Unable to deserialize data ", ex1);
        return null;
      }
      break;
    default:
      LOG.error("Unable to deserialize data. Doesn't appear to be JSON.");
      return null;
    }
    return eventList;
  }
  
}
