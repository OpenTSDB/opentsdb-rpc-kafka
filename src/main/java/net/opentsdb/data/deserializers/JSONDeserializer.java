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
