/**
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
 */
package org.apache.drill.exec.physical.impl.statspivot;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.PathSegment.NameSegment;
import org.apache.drill.common.expression.ValueExpressions.IntExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.Types;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnpivotStats;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VarCharVector.Mutator;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;

public class UnpivotStatsRecordBatch extends AbstractSingleRecordBatch<UnpivotStats>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnpivotStatsRecordBatch.class);
  
  String flattensrc = "statcount";
  String flattendest = "column";

  WritableBatch incomingData;
  VarCharVector keyvector;
  List<TransferPair> transfers = null;
  List<String> mapkeys = null;
  int keyindex = 0;

  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount;
  
  public UnpivotStatsRecordBatch(UnpivotStats pop, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(pop, context, incoming);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public IterOutcome innerNext() {
    if (hasRemainder) {
      handleRemainder();
      return IterOutcome.OK;
      
    } else if (keyindex != 0) {
      doWork();
      return IterOutcome.OK;
      
    } else {
      incomingData = incoming.getWritableBatch();
      incomingData.retainBuffers();
      incomingData.reconstructContainer(container);
      
      return super.innerNext();
    }
  }

  public VectorContainer getOutgoingContainer() {
    return this.container;
  }
  
  private void handleRemainder() {
    // TODO: handle remainder batch!!!
  }
  
  private int unpivotRecords() {
    int n;
//    Mutator m = keyvector.getMutator();
//    byte [] d = mapkeys.get(keyindex).getBytes();
//    
//    for (n = 0; n < incoming.getRecordCount(); n++) {
//      if (!m.setSafe(n, d)) {
//        break;
//      }
//    }
    // TODO: handle full keyvector!!
    System.out.println("Unpivoting");
    n = incoming.getRecordCount();
    
    for (TransferPair tp : transfers) {
      tp.splitAndTransfer(0, n);
    }
    
    return n;
  }

  @Override
  protected void doWork() {
    int outRecordCount = incoming.getRecordCount();
    int inRecordCount = unpivotRecords();
    
    if (inRecordCount < outRecordCount) {
      hasRemainder = true;
      remainderIndex = outRecordCount;
      this.recordCount = remainderIndex;
    } else {
      keyindex = (keyindex + 1) % mapkeys.size();
    }
    assert outRecordCount == incoming.getRecordCount();
    
    this.recordCount = outRecordCount;
  }
  
  @Override
  protected void setupNewSchema() throws SchemaChangeException{
    transfers = Lists.newArrayList();
    mapkeys = Lists.newArrayList();

    container.clear();

    if(incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.NONE){
      throw new UnsupportedOperationException("Selection vector not supported with statsPivot");
    }
    
    MajorType vctype = MajorType.newBuilder()
        .setMode(DataMode.REQUIRED)
        .setMinorType(MinorType.VARCHAR)
        .build();
//    keyvector = (VarCharVector) TypeHelper.getNewVector(
//        MaterializedField.create(flattendest, vctype),
//        oContext.getAllocator());
//    container.add(keyvector);
    
    MapVector mv = null;
    for (VectorWrapper<?> vw : incoming) {
      if (vw.getField().getLastName() == flattensrc) {
        if (mv != null) {
          throw new UnsupportedOperationException("multiple " + flattensrc + " found!");
        } else if (!(vw.getValueVector() instanceof MapVector)) {
          throw new UnsupportedOperationException("flatten not supported for type " + vw.getValueVector().getClass());
        }
        
        mv = (MapVector) vw.getValueVector();
        for (ValueVector vv : mv) {
          mapkeys.add(vv.getField().getLastName());
        }
      }
      
      System.out.println(vw.getField().getPath());
      TransferPair tp = vw.getValueVector().getTransferPair(
          new FieldReference(vw.getField().getPath()));
      container.add(tp.getTo());
      transfers.add(tp);
    }
    
    container.buildSchema(incoming.getSchema().getSelectionVectorMode());
  }
}
