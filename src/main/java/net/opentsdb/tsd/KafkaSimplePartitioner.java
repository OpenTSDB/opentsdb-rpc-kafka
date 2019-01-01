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

import kafka.utils.VerifiableProperties;

/**
 * Simple partitioner class. It assumes that our key is a positive integer 
 * based on the hash code. If you pass in a string that can't parse to a Long, 
 * then the partitioner will throw an exception.
 */
public class KafkaSimplePartitioner implements kafka.producer.Partitioner {

  /**
   * Ctor required for Kafka
   * @param vb Properties to parse if necessary
   */
  public KafkaSimplePartitioner(final VerifiableProperties vb) {

  }
  
  @Override
  public int partition(final Object key, final int num_partitions) {
    return (int)(Long.parseLong((String) key) % num_partitions);
  }

}
