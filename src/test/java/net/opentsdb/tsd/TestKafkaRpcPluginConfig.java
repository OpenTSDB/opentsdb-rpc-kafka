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

import net.opentsdb.utils.Config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class TestKafkaRpcPluginConfig {

  @Test
  public void defaults() throws Exception {
    final KafkaRpcPluginConfig config = 
        new KafkaRpcPluginConfig(new Config(false));
    
    assertEquals(KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_DEFAULT, 
        config.getInt(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.AUTO_COMMIT_INTERVAL_MS));
    assertTrue(config.getBoolean(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.AUTO_COMMIT_ENABLE));
    assertEquals(KafkaRpcPluginConfig.AUTO_OFFSET_RESET_DEFAULT, 
        config.getString(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.AUTO_OFFSET_RESET));
    assertEquals(KafkaRpcPluginConfig.REBALANCE_BACKOFF_MS_DEFAULT, 
        config.getInt(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.REBALANCE_BACKOFF_MS));
    assertEquals(KafkaRpcPluginConfig.REBALANCE_RETRIES_DEFAULT, 
        config.getInt(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.REBALANCE_RETRIES));
    assertEquals(KafkaRpcPluginConfig.ZK_SESSION_TIMEOUT_DEFAULT, 
        config.getInt(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.ZOOKEEPER_SESSION_TIMEOUT_MS));
    assertEquals(KafkaRpcPluginConfig.ZK_CONNECTION_TIMEOUT_DEFAULT, 
        config.getInt(KafkaRpcPluginConfig.KAFKA_CONFIG_PREFIX + 
            KafkaRpcPluginConfig.ZOOKEEPER_CONNECTION_TIMEOUT_MS));
    
    assertEquals(0, config.getInt(KafkaRpcPluginConfig.REQUIRED_ACKS));
    assertEquals(10000, config.getInt(KafkaRpcPluginConfig.REQUEST_TIMEOUT));
    assertEquals(1000, config.getInt(KafkaRpcPluginConfig.MAX_RETRIES));
    assertEquals("async", config.getString(KafkaRpcPluginConfig.PRODUCER_TYPE));
    assertEquals("kafka.serializer.StringEncoder", 
        config.getString(KafkaRpcPluginConfig.KEY_SERIALIZER));
    assertEquals("net.opentsdb.tsd.KafkaSimplePartitioner", 
        config.getString(KafkaRpcPluginConfig.PARTITIONER_CLASS));
  }
}
