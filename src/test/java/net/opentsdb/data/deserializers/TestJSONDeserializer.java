// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import net.opentsdb.data.Aggregate;
import net.opentsdb.data.Histogram;
import net.opentsdb.data.Metric;
import net.opentsdb.data.TypedIncomingData;
import net.opentsdb.data.deserializers.JSONDeserializer;
import net.opentsdb.tsd.KafkaRpcPluginThread;
import net.opentsdb.utils.JSON;

public class TestJSONDeserializer {
  private static final String METRIC = "sys.cpu.user";
  private static final long TS = 1492641000L;
  private Map<String, String> TAGS = ImmutableMap.<String, String>builder()
    .put("host", "web01")
    .build();
  
  private KafkaRpcPluginThread consumer;
  
  @Before
  public void before() throws Exception {
    consumer = mock(KafkaRpcPluginThread.class);
  }
  
  @Test
  public void ctor() throws Exception {
    // load it as we would in prod
    final Class<?> clazz = Class.forName(
        "net.opentsdb.data.deserializers.JSONDeserializer");
    assertNotNull(clazz);
    final Deserializer deserializer = 
        (Deserializer) clazz.getDeclaredConstructor().newInstance();
    assertTrue(deserializer instanceof JSONDeserializer);
  }
  
  @Test
  public void shutdown() throws Exception {
    final JSONDeserializer deserializer = new JSONDeserializer();
    assertNull(deserializer.shutdown().join());
  }
  
  @Test
  public void deserializeSingle() throws Exception {
    final JSONDeserializer deserializer = new JSONDeserializer();
    TypedIncomingData data = new Metric(METRIC, TS, "42", TAGS);
    
    List<TypedIncomingData> parsed = 
        deserializer.deserialize(consumer, JSON.serializeToBytes(data));
    assertEquals(1, parsed.size());
    assertEquals(METRIC, ((Metric) parsed.get(0)).getMetric());
    assertEquals(TS, ((Metric) parsed.get(0)).getTimestamp());
    assertEquals("42", ((Metric) parsed.get(0)).getValue());
    assertEquals("web01", ((Metric) parsed.get(0)).getTags().get("host"));
    assertEquals(0, parsed.get(0).getRequeueTS());
    
    // requeued
    data = new Metric(METRIC, TS, "24.5", TAGS);
    data.setRequeueTS(TS + 60);
    
    parsed = deserializer.deserialize(consumer, JSON.serializeToBytes(data));
    assertEquals(1, parsed.size());
    assertEquals(METRIC, ((Metric) parsed.get(0)).getMetric());
    assertEquals(TS, ((Metric) parsed.get(0)).getTimestamp());
    assertEquals("24.5", ((Metric) parsed.get(0)).getValue());
    assertEquals("web01", ((Metric) parsed.get(0)).getTags().get("host"));
    assertEquals(TS + 60, parsed.get(0).getRequeueTS());
    
    // Group By Aggregation
    data = new Aggregate(METRIC, TS, "42", TAGS, null, null, "sum");
    parsed = deserializer.deserialize(consumer, JSON.serializeToBytes(data));
    assertEquals(1, parsed.size());
    assertEquals(METRIC, ((Aggregate) parsed.get(0)).getMetric());
    assertEquals(TS, ((Aggregate) parsed.get(0)).getTimestamp());
    assertEquals("42", ((Aggregate) parsed.get(0)).getValue());
    assertEquals(0, parsed.get(0).getRequeueTS());
    assertEquals("web01", ((Aggregate) parsed.get(0)).getTags().get("host"));
    assertNull(((Aggregate) parsed.get(0)).getAggregator());
    assertNull(((Aggregate) parsed.get(0)).getInterval());
    assertEquals("sum", ((Aggregate) parsed.get(0)).getGroupByAggregator());
    
    // Rollup Aggregation
    data = new Aggregate(METRIC, TS, "42", TAGS, "1h", "sum", null);
    parsed = deserializer.deserialize(consumer, JSON.serializeToBytes(data));
    assertEquals(1, parsed.size());
    assertEquals(METRIC, ((Aggregate) parsed.get(0)).getMetric());
    assertEquals(TS, ((Aggregate) parsed.get(0)).getTimestamp());
    assertEquals("42", ((Aggregate) parsed.get(0)).getValue());
    assertEquals(0, parsed.get(0).getRequeueTS());
    assertEquals("web01", ((Aggregate) parsed.get(0)).getTags().get("host"));
    assertEquals("sum", ((Aggregate) parsed.get(0)).getAggregator());
    assertEquals("1h", ((Aggregate) parsed.get(0)).getInterval());
    assertNull(((Aggregate) parsed.get(0)).getGroupByAggregator());
    
    // Rollup Group By Aggregation
    data = new Aggregate(METRIC, TS, "42", TAGS, "1h", "sum", "max");
    parsed = deserializer.deserialize(consumer, JSON.serializeToBytes(data));
    assertEquals(1, parsed.size());
    assertEquals(METRIC, ((Aggregate) parsed.get(0)).getMetric());
    assertEquals(TS, ((Aggregate) parsed.get(0)).getTimestamp());
    assertEquals("42", ((Aggregate) parsed.get(0)).getValue());
    assertEquals(0, parsed.get(0).getRequeueTS());
    assertEquals("web01", ((Aggregate) parsed.get(0)).getTags().get("host"));
    assertEquals("sum", ((Aggregate) parsed.get(0)).getAggregator());
    assertEquals("1h", ((Aggregate) parsed.get(0)).getInterval());
    assertEquals("max", ((Aggregate) parsed.get(0)).getGroupByAggregator());
    
    // histogram
    final Histogram histo = new Histogram();
    histo.setMetric(METRIC);
    histo.setTimestamp(TS);
    histo.setTags(new HashMap<String, String>(TAGS));
    histo.setOverflow(1);
    histo.setBuckets(ImmutableMap.<String, Long>builder()
        .put("0,1", 42L)
        .put("1,5", 24L)
        .build());
    data = histo;
    parsed = deserializer.deserialize(consumer, JSON.serializeToBytes(data));
    assertEquals(1, parsed.size());
    assertEquals(METRIC, ((Histogram) parsed.get(0)).getMetric());
    assertEquals(TS, ((Histogram) parsed.get(0)).getTimestamp());
    assertNull(((Histogram) parsed.get(0)).getValue());
    assertEquals(42L, (long) ((Histogram) parsed.get(0)).getBuckets().get("0,1"));
  }
  
  @Test
  public void deserializeMulti() throws Exception {
    final JSONDeserializer deserializer = new JSONDeserializer();
    List<TypedIncomingData> data = Lists.newArrayList();
    data.add(new Metric(METRIC, TS, "42", TAGS));
    data.add(new Metric(METRIC, TS + 60, "24", TAGS));
    
    List<TypedIncomingData> parsed = 
        deserializer.deserialize(consumer, serialize(data));
    assertEquals(2, parsed.size());
    assertEquals(METRIC, ((Metric) parsed.get(0)).getMetric());
    assertEquals(TS, ((Metric) parsed.get(0)).getTimestamp());
    assertEquals("42", ((Metric) parsed.get(0)).getValue());
    assertEquals("web01", ((Metric) parsed.get(0)).getTags().get("host"));
    assertEquals(0, parsed.get(0).getRequeueTS());
    
    assertEquals(METRIC, ((Metric) parsed.get(1)).getMetric());
    assertEquals(TS + 60, ((Metric) parsed.get(1)).getTimestamp());
    assertEquals("24", ((Metric) parsed.get(1)).getValue());
    assertEquals("web01", ((Metric) parsed.get(1)).getTags().get("host"));
    assertEquals(0, parsed.get(1).getRequeueTS());
    
    // multiple-types
    final Histogram histo = new Histogram();
    histo.setMetric(METRIC);
    histo.setTimestamp(TS);
    histo.setTags(new HashMap<String, String>(TAGS));
    histo.setOverflow(1);
    histo.setBuckets(ImmutableMap.<String, Long>builder()
        .put("0,1", 42L)
        .put("1,5", 24L)
        .build());
    
    data.clear();
    data.add(new Aggregate(METRIC, TS, "42", TAGS, null, null, "sum"));
    data.add(new Aggregate(METRIC, TS, "42", TAGS, "1h", "sum", null));
    data.add(histo);
    parsed = deserializer.deserialize(consumer, serialize(data));
    
    assertEquals(3, parsed.size());
    assertEquals(METRIC, ((Aggregate) parsed.get(0)).getMetric());
    assertEquals(TS, ((Aggregate) parsed.get(0)).getTimestamp());
    assertEquals("42", ((Aggregate) parsed.get(0)).getValue());
    assertEquals(0, parsed.get(0).getRequeueTS());
    assertEquals("web01", ((Aggregate) parsed.get(0)).getTags().get("host"));
    assertNull(((Aggregate) parsed.get(0)).getAggregator());
    assertNull(((Aggregate) parsed.get(0)).getInterval());
    assertEquals("sum", ((Aggregate) parsed.get(0)).getGroupByAggregator());
    
    assertEquals(METRIC, ((Aggregate) parsed.get(1)).getMetric());
    assertEquals(TS, ((Aggregate) parsed.get(1)).getTimestamp());
    assertEquals("42", ((Aggregate) parsed.get(1)).getValue());
    assertEquals(0, parsed.get(1).getRequeueTS());
    assertEquals("web01", ((Aggregate) parsed.get(1)).getTags().get("host"));
    assertEquals("sum", ((Aggregate) parsed.get(1)).getAggregator());
    assertEquals("1h", ((Aggregate) parsed.get(1)).getInterval());
    assertNull(((Aggregate) parsed.get(1)).getGroupByAggregator());
    
    assertEquals(METRIC, ((Histogram) parsed.get(2)).getMetric());
    assertEquals(TS, ((Histogram) parsed.get(2)).getTimestamp());
    assertNull(((Histogram) parsed.get(2)).getValue());
    assertEquals(42L, (long) ((Histogram) parsed.get(2)).getBuckets().get("0,1"));
  }
  
  @Test
  public void deserializeErrors() throws Exception {
    final JSONDeserializer deserializer = new JSONDeserializer();
    
    assertNull(deserializer.deserialize(consumer, null));
    assertNull(deserializer.deserialize(consumer, new byte[0]));
    assertNull(deserializer.deserialize(consumer, "Not JSON".getBytes()));
    assertNull(deserializer.deserialize(consumer, "{\"key\":\"va".getBytes()));
  }
  
  private byte[] serialize(final List<TypedIncomingData> data) throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final JsonGenerator json = JSON.getFactory().createGenerator(output);
    json.writeStartArray();
    for (final TypedIncomingData d : data) {
      json.writeObject(d);
    }
    json.writeEndArray();
    json.close();
    return output.toByteArray();
  }
}
