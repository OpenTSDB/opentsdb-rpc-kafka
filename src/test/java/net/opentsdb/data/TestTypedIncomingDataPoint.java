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
package net.opentsdb.data;

import org.junit.Test;

import net.opentsdb.data.TypedIncomingData;
import net.opentsdb.utils.JSON;

public class TestTypedIncomingDataPoint {

  @Test
  public void foo() throws Exception {
    String json = "{\"type\":\"Histogram\",\"metric\":\"sys.cpu.user\",\"id\":42}";
    
    TypedIncomingData dp = JSON.parseToObject(json, TypedIncomingData.class);
    System.out.println("Type: " + dp.getClass());
    System.out.println(dp);
    
    System.out.println(JSON.serializeToString(dp));
  }
}
