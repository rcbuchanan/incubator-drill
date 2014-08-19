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

import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnpivotStats;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.MapVector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UnpivotStatsRecordBatch extends AbstractSingleRecordBatch<UnpivotStats>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnpivotStatsRecordBatch.class);
  
  String keysrc = "statcount";
  String keydest = "column";
  List<String> datasrcs = Lists.newArrayList(
      "statcount",
      "nonnullstatcount",
      "ndv",
      "hll");

  WritableBatch incomingData;
  VarCharVector keyVec;
  int keyIndex = 0;
  List<String> keyList = null;
  Map<String, Map<String, ValueVector>> dataSrcVecMap = null;
  Map<String, ValueVector> dataDestVecMap;
  
  List<ValueVector> transferSrcList;
  List<TransferPair> transferList;

  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount = 0;
  
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
      
    } else if (keyIndex != 0) {
      doWork();
      return IterOutcome.OK;
      
    } else {
      return super.innerNext();
      
    }
  }

  public VectorContainer getOutgoingContainer() {
    return this.container;
  }
  
  private void handleRemainder() {
    // TODO: handle remainder batch!!!
  }
  
  private int doTransfer() {
    int n;

    n = incoming.getRecordCount();
    
    for (TransferPair tp : transferList) {
      tp.splitAndTransfer(0, n);
    }
    
    return n;
  }

  @Override
  protected void doWork() {
    int outRecordCount = incoming.getRecordCount();
    int inRecordCount = doTransfer();
    
    if (inRecordCount < outRecordCount) {
      hasRemainder = true;
      remainderIndex = outRecordCount;
      this.recordCount = remainderIndex;
    } else {
      keyIndex = (keyIndex + 1) % keyList.size();
      prepareTransfers();
    }
    this.recordCount = outRecordCount;
  }
  
  private void buildKeyList() {
    for (VectorWrapper<?> vw : incoming) {
      String ks = vw.getField().getLastName();
      
      if (ks != keysrc) {
        continue;
      }
      
      assert keyList == null;
      keyList = Lists.newArrayList();
      
      for (ValueVector vv : (MapVector) vw.getValueVector()) {
        keyList.add(vv.getField().getLastName());
      }
    }
  }
  
  private void buildDataSrcMap() {
    dataSrcVecMap = Maps.newHashMap();
    for (VectorWrapper<?> vw : incoming) {
      String ds = vw.getField().getLastName();
      
      if (!datasrcs.contains(ds)) {
        continue;
      }
      
      assert !dataSrcVecMap.containsKey(ds);
      Map<String, ValueVector> m = Maps.newHashMap();
      dataSrcVecMap.put(ds, m);
      
      for (ValueVector vv : (MapVector) vw.getValueVector()) {
        String k = vv.getField().getLastName();
        
        if (!keyList.contains(k)) {
          throw new UnsupportedOperationException("Unpivot data vector " +
              ds + " contains key " + k + " not contained in key source!");
        }
        
        if (vv.getField().getType().getMinorType() == MinorType.MAP) {
          throw new UnsupportedOperationException("Unpivot of nested map is " +
              "not supported!");
        }
        
        m.put(vv.getField().getLastName(), vv);
      }
    }
  }
  
  private void prepareTransfers() {
    container.clear();
    
    transferList = Lists.newArrayList();
    transferSrcList = Lists.newArrayList();
    for (VectorWrapper<?> vw : incoming) {
      String col = vw.getField().getLastName();
      
      ValueVector vv;
      TransferPair tp;
      if (datasrcs.contains(col)) {
        String k = keyList.get(keyIndex);
        vv = dataSrcVecMap.get(col).get(k);
        tp = vv.getTransferPair(new FieldReference(col));
      } else {
        vv = vw.getValueVector();
        tp = vv.getTransferPair(new FieldReference(col));
      }
      
      container.add(tp.getTo());
      transferSrcList.add(vv);
      transferList.add(tp);
    }
    
    container.buildSchema(incoming.getSchema().getSelectionVectorMode());
  }
  
  @Override
  protected void setupNewSchema() throws SchemaChangeException {
    if(incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.NONE){
      throw new UnsupportedOperationException("Selection vector not supported with statsPivot");
    }
    
    buildKeyList();
    buildDataSrcMap();
    prepareTransfers();
  }
}
