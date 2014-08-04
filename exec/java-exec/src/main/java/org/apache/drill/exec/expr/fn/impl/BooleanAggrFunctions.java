
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

import java.io.IOException;
import java.io.InputStream;

import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
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
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

@SuppressWarnings("unused")
public class BooleanAggrFunctions {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BooleanAggrFunctions.class);
  
  @FunctionTemplate(name = "tfunc", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class TFunc implements DrillAggFunc{

    @Param FieldReader in;
    @Output BigIntHolder out;

    public void setup(RecordBatch b) {
    }

    @Override
    public void add() {
      int  i = 0;
      for (String s : in) {
        System.out.println(s + ", " + in.reader(s).getType());
      }
    }

    @Override
    public void output() {
    }

    @Override
    public void reset() {
    }
  }
  
  @FunctionTemplate(name = "hll", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllAggregate implements DrillAggFunc{

    @Param NullableVarCharHolder in;
    @Workspace VarBinaryHolder data;
    @Output VarBinaryHolder out;

    public void setup(RecordBatch b) {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll = 
          new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
      data = new VarBinaryHolder();

      try {
        byte [] ba = hll.getBytes();
        data.buffer = io.netty.buffer.Unpooled.wrappedBuffer(ba);
        data.start = 0;
        data.end = ba.length;
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void add() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll;
      try {
        hll = com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(data.buffer.array());
        hll.offer(in.toString());
        data.buffer.setBytes(data.start, hll.getBytes());
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void output() {
      out.start = data.start;
      out.buffer = data.buffer;
      out.end = data.end;
    }

    @Override
    public void reset() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll = 
          new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);      
      try {
        data.buffer.setBytes(data.start, hll.getBytes());
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  @FunctionTemplate(name = "hll_decode", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class HllDecode implements DrillSimpleFunc{

    @Param VarBinaryHolder in;
    @Output BigIntHolder out;
    @Workspace java.util.regex.Pattern regPattern;

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
  
  @FunctionTemplate(name = "hll_combine", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
  public static class HllCombineAggregate implements DrillAggFunc{

    @Param VarBinaryHolder in;
    @Workspace VarBinaryHolder data;
    @Output VarBinaryHolder out;

    public void setup(RecordBatch b) {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll = 
          new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);
      data = new VarBinaryHolder();
      
      try {
        byte [] ba = hll.getBytes();
        data.buffer = io.netty.buffer.Unpooled.wrappedBuffer(ba);
        data.start = 0;
        data.end = ba.length;
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void add() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll;
      try {
        hll = com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(data.buffer.array());
        
        byte [] din = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, din);
        
        hll.addAll(com.clearspring.analytics.stream.cardinality.HyperLogLog.Builder.build(din));
        
        data.buffer.setBytes(data.start, hll.getBytes());
      } catch (java.io.IOException e1) {
        e1.printStackTrace();
      } catch (com.clearspring.analytics.stream.cardinality.CardinalityMergeException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void output() {
      out.start = data.start;
      out.buffer = data.buffer;
      out.end = data.end;
    }

    @Override
    public void reset() {
      com.clearspring.analytics.stream.cardinality.HyperLogLog hll = 
          new com.clearspring.analytics.stream.cardinality.HyperLogLog(10);      
      try {
        data.buffer.setBytes(data.start, hll.getBytes());
      } catch (java.io.IOException e) {
        e.printStackTrace();
      }
    }
  }
  
@FunctionTemplate(name = "bool_or", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class BitBooleanOr implements DrillAggFunc{

  @Param BitHolder in;
  @Workspace BitHolder inter;
  @Output BitHolder out;

  public void setup(RecordBatch b) {
  inter = new BitHolder();

    // Initialize the workspace variables
    inter.value = 0;
  }

  @Override
  public void add() {
    inter.value = inter.value | in.value;
  }

  @Override
  public void output() {
    out.value = inter.value;
  }

  @Override
  public void reset() {
    inter.value = 0;
  }
}



@FunctionTemplate(name = "bool_or", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableBitBooleanOr implements DrillAggFunc{

  @Param NullableBitHolder in;
  @Workspace BitHolder inter;
  @Output BitHolder out;

  public void setup(RecordBatch b) {
  inter = new BitHolder();

    // Initialize the workspace variables
    inter.value = 0;
  }

  @Override
  public void add() {
    sout: {
    if (in.isSet == 0) {
     // processing nullable input and the value is null, so don't do anything...
     break sout;
    }

    inter.value = inter.value | in.value;
    } // end of sout block
  }


  @Override
  public void output() {
    out.value = inter.value;
  }

  @Override
  public void reset() {
    inter.value = 0;
  }
}


@FunctionTemplate(name = "bool_and", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class BitBooleanAnd implements DrillAggFunc{

  @Param BitHolder in;
  @Workspace BitHolder inter;
  @Output BitHolder out;

  public void setup(RecordBatch b) {
  inter = new BitHolder();

    // Initialize the workspace variables
    inter.value = Integer.MAX_VALUE;
  }

  @Override
  public void add() {

    inter.value = inter.value & in.value;

  }

  @Override
  public void output() {
    out.value = inter.value;
  }

  @Override
  public void reset() {
    inter.value = Integer.MAX_VALUE;
  }
}


@FunctionTemplate(name = "bool_and", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class NullableBitBooleanAnd implements DrillAggFunc{

  @Param NullableBitHolder in;
  @Workspace BitHolder inter;
  @Output BitHolder out;

  public void setup(RecordBatch b) {
  inter = new BitHolder();

    // Initialize the workspace variables
    inter.value = Integer.MAX_VALUE;
  }

  @Override
  public void add() {
    sout: {
    if (in.isSet == 0) {
     // processing nullable input and the value is null, so don't do anything...
     break sout;
    }

    inter.value = inter.value & in.value;

    } // end of sout block
  }


  @Override
  public void output() {
    out.value = inter.value;
  }

  @Override
  public void reset() {
    inter.value = Integer.MAX_VALUE;
  }
}

}