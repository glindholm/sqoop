/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop;

import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSqoopOptions {

  @Test
  public void testParseColumnParsing() {
    new SqoopOptions() {
  @Test
      public void testParseColumnMapping() {
        Properties result = new Properties();
        parseColumnMapping("test=INTEGER,test1=DECIMAL(1%2C1),test2=NUMERIC(1%2C%202)", result);
        assertEquals("INTEGER", result.getProperty("test"));
        assertEquals("DECIMAL(1,1)", result.getProperty("test1"));
        assertEquals("NUMERIC(1, 2)", result.getProperty("test2"));
      }
    }.testParseColumnMapping();
  }

  @Test
  public void testColumnNameCaseInsensitive() {
    SqoopOptions opts = new SqoopOptions();
    opts.setColumns(new String[]{ "AAA", "bbb" });
    assertEquals("AAA", opts.getColumnNameCaseInsensitive("aAa"));
    assertEquals("bbb", opts.getColumnNameCaseInsensitive("BbB"));
    assertEquals(null, opts.getColumnNameCaseInsensitive("notFound"));
    opts.setColumns(null);
    assertEquals(null, opts.getColumnNameCaseInsensitive("noColumns"));
  }
  
  @Test
  public void testParseMapTypeHCat() {
      
    Map<String, String> map = SqoopOptions.parseMapTypeHCat("Timestamp(26,6)=TIMESTAMP,Date=Xyz(7,2),Time(8)=Time(8,0)");
    //System.out.println(map.toString());
    assertEquals(3, map.size());
    assertEquals("TIMESTAMP", map.get("(93,26,6)"));
    assertEquals("Xyz(7,2)", map.get("(91)"));
    assertEquals("Time(8,0)", map.get("(92,8)"));  
  }
  
  @Test
  public void testParseMapTypeHCatErrors() {
    
    // Malformed map
    try {
      SqoopOptions.parseMapTypeHCat("x,y,z");
      fail("Malformed map");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Malformed"));
    }
    
    // Invalid sql type format
    try {
      SqoopOptions.parseMapTypeHCat("time(*)=time");
      fail("Invalid sql type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid"));
    }
    
    // Unrecognized sql type
    try {
      SqoopOptions.parseMapTypeHCat("XXX=time");
      fail("Unrecognized sql type");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unrecognized"));
    }
  }

  @Test
  public void testLookupMapTypeHive() {
    SqoopOptions opts = new SqoopOptions();
    opts.setMapTypeHCat("Timestamp(26,6)=TIMESTAMP,Date=Xyz(7,2),Time(8)=Time(8,0),TIME=TIME");
    
    assertEquals("TIMESTAMP", opts.lookupMapTypeHCat(93, 26, 6));
    assertEquals("Xyz(7,2)", opts.lookupMapTypeHCat(91, 0, 0));
    assertEquals("Time(8,0)", opts.lookupMapTypeHCat(92, 8, 0));
    assertEquals("TIME", opts.lookupMapTypeHCat(92, 0, 0));
  
    assertEquals(null, opts.lookupMapTypeHCat(99999, 0, 0));
  }
  
}
