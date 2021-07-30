// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.opentsdb.core.IncomingDataPoint;
import net.opentsdb.data.Metric;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class KafkaDummyDataGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaDummyDataGenerator.class);

  static String[] metrics = {
          "kafka.dummy.data.m1",
          "kafka.dummy.data.m2",
          "kafka.dummy.data.m3",
          "kafka.dummy.data.m4",
          "kafka.dummy.data.m5",
  };

  public static void main(final String[] args) {
    // hacky and ugly
    String configPath = null;
    if (args != null && args.length > 0) {
      for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("--config")) {
          configPath = args[i].substring(9);
        }
      }
    }

    final KafkaRpcPluginConfig config;
    try {
      if (Strings.isNullOrEmpty(configPath)) {
        config = new KafkaRpcPluginConfig(new Config(true));
      } else {
        config = new KafkaRpcPluginConfig(new Config(configPath));
      }

      String temp = config.getString(configKey("frequency"));
      if (temp == null) {
        temp = "1m";
      }
      long interval = DateTime.parseDuration(temp);
      if (interval < 1) {
        System.err.println("Interval must be greater than 0.");
        System.exit(1);
      }
      LOG.info("Writing values every " + temp);

      temp = config.getString(configKey("topic"));
      if (Strings.isNullOrEmpty(temp)) {
        System.err.println(configKey("topic") + " cannot be null or empty.");
        System.exit(1);
      }
      final String topic = temp;

      final int tags;
      temp = config.getString(configKey("hosts"));
      if (!Strings.isNullOrEmpty(temp)) {
        tags = Integer.parseInt(temp);
      } else {
        tags = 5;
      }

      final Properties properties = new Properties();
      for (Map.Entry<String, String> entry : config.getMap().entrySet()) {
        final String key = entry.getKey();
        if (entry.getKey().startsWith("kafka.dummyProducer.kafka.")) {
          properties.put(key.substring(
                  key.indexOf("kafka.dummyProducer.kafka.") +
                          "kafka.dummyProducer.kafka.".length()),
                  entry.getValue());
        }
      }

      final ProducerConfig producerConfig = new ProducerConfig(properties);
      final Producer producer = new Producer<String, byte[]>(producerConfig);

      final Random rnd = new Random(System.currentTimeMillis());
      while (true) {
        long now = System.currentTimeMillis() / 1000;
        for (int i = 0; i < metrics.length; i++) {
          String metric = metrics[i];
          for (int x = 0; x < tags; x++) {
            String host = String.format("web%02d", x+ 1);
            Metric dp = new Metric(
                    metric,
                    now,
                    rnd.nextInt(99) + "." + rnd.nextInt(99),
                    ImmutableMap.of("host", host, "dc", "DEN")
            );
            int hash = dp.getMetric().hashCode() + Objects.hashCode(dp.getTags());
            final KeyedMessage<byte[], byte[]> data =
                    new KeyedMessage<byte[], byte[]>(topic,
                            Integer.toString(Math.abs(hash)).getBytes(),
                            JSON.serializeToBytes(dp));
            producer.send(data);
          }
        }
        LOG.info("Sent {} metrics and {} tags for timestamp {}",
                metrics.length, tags, now);
        Thread.sleep(interval);
      }

    } catch (IOException e) {
      e.printStackTrace();
      System.exit(2);
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(2);
    }
  }

  static String configKey(final String suffix) {
    return "kafka.dummyProducer." + suffix;
  }
}
