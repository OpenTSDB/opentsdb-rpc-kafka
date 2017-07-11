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
