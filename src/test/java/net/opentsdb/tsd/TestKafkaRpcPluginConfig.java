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
