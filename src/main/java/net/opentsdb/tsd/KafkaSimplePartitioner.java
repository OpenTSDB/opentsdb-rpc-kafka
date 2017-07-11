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
