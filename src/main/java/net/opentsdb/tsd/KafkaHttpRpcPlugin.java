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
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tools.BuildData;
import net.opentsdb.utils.JSON;

public class KafkaHttpRpcPlugin extends HttpRpcPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaHttpRpcPlugin.class);
  
  private TSDB tsdb;
  
  @Override
  public void initialize(TSDB tsdb) {
    this.tsdb = tsdb;
    LOG.info("Initialized KafkaHttpRpcPlugin");
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "2.4.0";
  }

  @Override
  public void collectStats(StatsCollector collector) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public String getPath() {
    return "kafkarpc";
  }

  @Override
  public void execute(final TSDB tsdb, final HttpRpcPluginQuery query) throws IOException {
    // only accept GET/POST for now
    if (query.request().getMethod() != HttpMethod.GET && 
        query.request().getMethod() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    final String[] uri = query.explodePath();
    final String endpoint = uri.length > 1 ? uri[2].toLowerCase() : "";
    
    if ("version".equals(endpoint)) {
      handleVersion(query);
    } else if ("rate".equals(endpoint)) {
      handleRate(query);
    } else if ("namespace".equals(endpoint)) {
      handlePerNamespaceStats(query);
    } else if ("perthread".equals(endpoint)) {
      handlePerThreadStats(query);
    } else {
      throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
          "Hello. You have reached an API that has been disconnected. "
          + "Please call again.");
    }
  }

  /**
   * Prints Version information for the various components.
   * @param query A non-null HTTP query to parse and respond to.
   */
  private void handleVersion(final HttpRpcPluginQuery query) {
    final Map<String, Map<String, String>> versions = Maps.newHashMap();
    
    Map<String, String> version = Maps.newHashMap();
    version.put("version", BuildData.version);
    version.put("short_revision", BuildData.short_revision);
    version.put("full_revision", BuildData.full_revision);
    version.put("timestamp", Long.toString(BuildData.timestamp));
    version.put("repo_status", BuildData.repo_status.toString());
    version.put("user", BuildData.user);
    version.put("host", BuildData.host);
    version.put("repo", BuildData.repo);
    versions.put("tsdb", version);

    // TODO - plugin version
    
    query.sendBuffer(HttpResponseStatus.OK,
        ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(versions)),
        "application/json");
  }
  
  /**
   * Handles calls to print and/or adjust the rate per group
   * @param query The HTTP query to parse and respond to.
   */
  private void handleRate(final HttpRpcPluginQuery query) {
    synchronized (tsdb) {
      if (KafkaRpcPlugin.KAFKA_RPC_REFERENCE == null) {
        throw new BadRequestException(HttpResponseStatus.CONFLICT, 
            "Consumers have not started yet");
      }
      // for now we'll just parse URI params to cheat
      final String group_id = query.getQueryStringParam("group");
      final double rate;
      if (group_id != null && !group_id.isEmpty()) {
        rate = Double.parseDouble(query.getRequiredQueryStringParam("rate"));
        KafkaRpcPlugin.KAFKA_RPC_REFERENCE.setRate(group_id, rate);
      } else {
        rate = 0;
      }
      
      final Map<String, Double> rates = 
          KafkaRpcPlugin.KAFKA_RPC_REFERENCE.getRates();
      query.sendBuffer(HttpResponseStatus.OK,
          ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(rates)),
          "application/json");
    }
  }
  
  /**
   * Handles printing stats per namespace. Aggregated stats are included with 
   * the main tsdb /api/stats call. Users can filter by namespace if they want. 
   * @param query The HTTP query to parse and respond to.
   */
  private void handlePerNamespaceStats(final HttpRpcPluginQuery query) {
    synchronized (tsdb) {
      if (KafkaRpcPlugin.KAFKA_RPC_REFERENCE == null) {
        throw new BadRequestException(HttpResponseStatus.CONFLICT, 
            "Consumers have not started yet");
      }
      
      final String filter = query.getQueryStringParam("namespace");
      final Map<String, Map<String, AtomicLong>> counters = 
          KafkaRpcPlugin.KAFKA_RPC_REFERENCE.getNamespaceCounters();
      final long ts = System.currentTimeMillis() / 1000;
      
      // TSD format for ingest
      final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
      for (final Entry<String, Map<String, AtomicLong>> counter : 
        counters.entrySet()) {
        
        for (final Entry<String, AtomicLong> metric : counter.getValue().entrySet()) {
          if (filter != null && !filter.isEmpty() && 
              !filter.toLowerCase().equals(metric.getKey().toLowerCase())) {
            continue;
          }
          
          final Map<String, Object> entry = new HashMap<String, Object>(4);
          entry.put("metric", "KafkaRpcPlugin.perNamespace." + counter.getKey());
          entry.put("timestamp", ts);
          entry.put("value", metric.getValue().get());
          final Map<String, String> tags = new HashMap<String, String>(2);
          tags.put("host", KafkaRpcPlugin.KAFKA_RPC_REFERENCE.getHost());
          tags.put("namespace", metric.getKey());
          entry.put("tags", tags);
          results.add(entry);
        }
      }
      query.sendBuffer(HttpResponseStatus.OK,
          ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(results)),
          "application/json");
    }
  }

  /** Publish per thread stats */
  private void handlePerThreadStats(final HttpRpcPluginQuery query) {
    synchronized (tsdb) {
      if (KafkaRpcPlugin.KAFKA_RPC_REFERENCE == null) {
        throw new BadRequestException(HttpResponseStatus.CONFLICT, 
            "Consumers have not started yet");
      }
      final long ts = System.currentTimeMillis() / 1000;
      
      Map<String, Map<Integer, Map<String, Double>>> stats = 
          KafkaRpcPlugin.KAFKA_RPC_REFERENCE.getPerThreadStats();
      // TSD format for ingest
      final List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
      for (final Entry<String, Map<Integer, Map<String, Double>>> group : stats.entrySet()) {
        for (final Entry<Integer, Map<String, Double>> thread : group.getValue().entrySet()) {
          for (final Entry<String, Double> counter : thread.getValue().entrySet()) {
            final Map<String, Object> entry = new HashMap<String, Object>(4);
            entry.put("metric", "KafkaRpcPlugin.perThread." + counter.getKey());
            entry.put("timestamp", ts);
            entry.put("value", counter.getValue());
            final Map<String, String> tags = new HashMap<String, String>(2);
            tags.put("host", KafkaRpcPlugin.KAFKA_RPC_REFERENCE.getHost());
            tags.put("consumer", group.getKey());
            tags.put("thread", Integer.toString(thread.getKey()));
            entry.put("tags", tags);
            results.add(entry);
          }
        }
        query.sendBuffer(HttpResponseStatus.OK,
            ChannelBuffers.wrappedBuffer(JSON.serializeToBytes(results)),
            "application/json");
      }
    }
  }
  
}
