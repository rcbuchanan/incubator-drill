/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

/*
 * This class is automatically generated from AggrTypeFunctions2.tdd using FreeMarker.
 */

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.ObjectHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.NullableSmallIntHolder;
import org.apache.drill.exec.expr.holders.TinyIntHolder;
import org.apache.drill.exec.expr.holders.NullableTinyIntHolder;
import org.apache.drill.exec.expr.holders.UInt1Holder;
import org.apache.drill.exec.expr.holders.NullableUInt1Holder;
import org.apache.drill.exec.expr.holders.UInt2Holder;
import org.apache.drill.exec.expr.holders.NullableUInt2Holder;
import org.apache.drill.exec.expr.holders.UInt4Holder;
import org.apache.drill.exec.expr.holders.NullableUInt4Holder;
import org.apache.drill.exec.expr.holders.UInt8Holder;
import org.apache.drill.exec.expr.holders.NullableUInt8Holder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

@SuppressWarnings("unused")
public class StatisticsAggrFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsAggrFunctions.class);

  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllNullableVarBinary implements DrillAggFunc {
    @Param NullableVarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;

    public void setup(RecordBatch b) {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
    }

    @Override
    public void add() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
      if (in.isSet()) {
        byte [] d = new byte[in.end - in.start]; 
        in.buffer.getBytes(in.start, d);
        hll.offer(d);
      } else {
        hll.offer(null);
      }
    }

    @Override
    public void output() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

      try {
        byte [] ba = hll.getBytes();
        out.buffer = io.netty.buffer.Unpooled.wrappedBuffer(ba);
        out.start = 0;
        out.end = ba.length;
        out.isSet = 1;
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
    }
  }
  
  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllVarBinary implements DrillAggFunc {
    @Param VarBinaryHolder in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;

    public void setup(RecordBatch b) {
      work = new ObjectHolder();
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
    }

    @Override
    public void add() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;
      byte [] d = new byte[in.end - in.start]; 
      in.buffer.getBytes(in.start, d);
      hll.offer(d);
    }

    @Override
    public void output() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll =
          (com.clearspring.analytics.stream.cardinality.HyperLogLog) work.obj;

      try {
        byte [] ba = hll.getBytes();
        out.buffer = io.netty.buffer.Unpooled.wrappedBuffer(ba);
        out.start = 0;
        out.end = ba.length;
        out.isSet = 1;
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void reset() {
      work.obj = new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
    }
  }
  
  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllDummy implements DrillAggFunc {
    @Param FieldReader in;
    @Workspace ObjectHolder work;
    @Output NullableVarBinaryHolder out;

    public void setup(RecordBatch b) {
    }

    @Override
    public void add() {
    }

    @Override
    public void output() {
      out.buffer = null;
      out.start = out.end = out.isSet = 0;
    }

    @Override
    public void reset() {
    }
  }
  
  @FunctionTemplate(name = "hll_decode", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class HllDecode implements DrillSimpleFunc {

    @Param NullableVarBinaryHolder in;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming){
    }

    public void eval(){
      byte [] din = new byte[in.end - in.start];
      in.buffer.getBytes(in.start, din);
      
      try {
        out.value = com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(din).cardinality();
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }
  }


}