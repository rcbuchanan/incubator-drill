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
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;

public class UnpivotStatsRecordBatch extends AbstractSingleRecordBatch<UnpivotStats>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnpivotStatsRecordBatch.class);

  private StatsUnpivotor statsPivotor;
  private List<ValueVector> allocationVectors;
  private List<ComplexWriter> complexWriters;
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
    }
    return super.innerNext();
  }

  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  @Override
  protected void doWork() {
    // VectorUtil.showVectorAccessibleContent(incoming, ",");
    int incomingRecordCount = incoming.getRecordCount();

    doAlloc();
    
    System.out.println("INCOMING " + incomingRecordCount);

    int outputRecords = statsPivotor.pivotRecords(0, incomingRecordCount, 0);
    System.out.println("OUTPUTTED " + outputRecords);
    
    if (outputRecords < incomingRecordCount * statsPivotor.getRecordsPerRecord()) {
      setValueCount(outputRecords);
      hasRemainder = true;
      remainderIndex = outputRecords;
      this.recordCount = remainderIndex;
    } else {
      setValueCount(outputRecords);
      for(VectorWrapper<?> v: incoming) {
        v.clear();
      }
      this.recordCount = outputRecords;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }
  }

  private void handleRemainder() {
    int remainingRecordCount = incoming.getRecordCount() * statsPivotor.getRecordsPerRecord() - remainderIndex;
    doAlloc();
    int outputRecords = statsPivotor.pivotRecords(remainderIndex, remainingRecordCount, 0);
    if (outputRecords < remainingRecordCount * statsPivotor.getRecordsPerRecord()) {
      setValueCount(outputRecords);
      this.recordCount = outputRecords;
      remainderIndex += outputRecords;
    } else {
      setValueCount(outputRecords);
      hasRemainder = false;
      remainderIndex = 0;
      for(VectorWrapper<?> v: incoming) {
        v.clear();
      }
      this.recordCount = outputRecords;
    }
    // In case of complex writer expression, vectors would be added to batch run-time.
    // We have to re-build the schema.
    if (complexWriters != null) {
      container.buildSchema(SelectionVectorMode.NONE);
    }
  }

  public void addComplexWriter(ComplexWriter writer) {
    complexWriters.add(writer);
  }

  private boolean doAlloc() {
    //Allocate vv in the allocationVectors.
    for(ValueVector v : this.allocationVectors){
      //AllocationHelper.allocate(v, remainingRecordCount, 250);
      if (!v.allocateNewSafe())
        return false;
    }

    //Allocate vv for complexWriters.
    if (complexWriters == null)
      return true;

    for (ComplexWriter writer : complexWriters)
      writer.allocate();

    return true;
  }

  private void setValueCount(int count) {
    for(ValueVector v : allocationVectors){
      ValueVector.Mutator m = v.getMutator();
      m.setValueCount(count);
    }

    if (complexWriters == null)
      return;

    for (ComplexWriter writer : complexWriters)
      writer.setValueCount(count);
  }

  /** hack to make ref and full work together... need to figure out if this is still necessary. **/
  private FieldReference getRef(NamedExpression e){
    FieldReference ref = e.getRef();
    PathSegment seg = ref.getRootSegment();

//    if(seg.isNamed() && "output".contentEquals(seg.getNameSegment().getPath())){
//      return new FieldReference(ref.getPath().toString().subSequence(7, ref.getPath().length()), ref.getPosition());
//    }
    return ref;
  }

  private boolean isAnyWildcard(List<NamedExpression> exprs){
    for(NamedExpression e : exprs){
      if(isWildcard(e)) return true;
    }
    return false;
  }

  private boolean isWildcard(NamedExpression ex){
    if( !(ex.getExpr() instanceof SchemaPath)) return false;
    NameSegment expr = ((SchemaPath)ex.getExpr()).getRootSegment();
    NameSegment ref = ex.getRef().getRootSegment();
    return ref.getPath().equals("*") && expr.getPath().equals("*");
  }
  
  private int lookupOrAddCol(String colName, MajorType colType, List<String> colNames, List<MajorType> colTypes) {
    int rv = colNames.indexOf(colName);
    if (rv == -1) {
      rv = colNames.size();
      colNames.add(colName);
      colTypes.add(colType);
    }
    return rv;
  }

  private int lookupOrAddSubrec(String subrecName, List<String> subrecNames, List<Integer> subrecCounts) {
    int rv = subrecNames.indexOf(subrecName);
    if (rv == -1) {
      rv = subrecNames.size();
      subrecNames.add(subrecName);
      subrecCounts.add(0);
    }
    subrecCounts.set(rv, subrecCounts.get(rv) + 1);
    return rv;
  }
  
  private MaterializedField addOutputCol(String name, MajorType type) {
    MaterializedField field = MaterializedField.create(new FieldReference(name), type);
    ValueVector vector = TypeHelper.getNewVector(field, oContext.getAllocator());
    allocationVectors.add(vector);
    container.add(vector);
    return field;
  }
  
  private void codegenAddExpression(ClassGenerator<StatsUnpivotor> cg, LogicalExpression expr, int subrecCount, int subrec) {
    HoldingContainer hc = cg.addExpr(expr, false);
    JBlock b = cg.getEvalBlock();
    cg.unNestEvalBlock();
    b._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    JBlock ifed = new JBlock(true, true);
    ifed._if(JExpr.ref("outIndex").mod(JExpr.lit(subrecCount)).eq(JExpr.lit(subrec)))._then().add(b);
    cg.nestEvalBlock(ifed);
    cg.rotateBlock();
  }
  
  private LogicalExpression makeInToOutExpression(int inIndex, MaterializedField outField) {
    SchemaPath inPath = incoming.getSchema().getColumn(inIndex).getPath();
    SchemaPath outPath = outField.getPath();
    TypedFieldId outFid = container.getValueVectorId(outPath);
    final ErrorCollector collector = new ErrorCollectorImpl();

    ValueVectorWriteExpression write = new ValueVectorWriteExpression(
        outFid,
        ExpressionTreeMaterializer.materialize(
            inPath, incoming, collector,
            context.getFunctionRegistry(), true),
        true);
    
    return write;
  }
  
  private LogicalExpression makeColumnNameExpression(String colName, MaterializedField outField) {
    SchemaPath outPath = outField.getPath();
    TypedFieldId outFid = container.getValueVectorId(outPath);
    final ErrorCollector collector = new ErrorCollectorImpl();

    ValueVectorWriteExpression write = new ValueVectorWriteExpression(
        outFid,
        ExpressionTreeMaterializer.materialize(
            ValueExpressions.getChar(colName), incoming, collector,
            context.getFunctionRegistry(), true),
        true);
    
    return write;
  }
  
  @Override
  protected void setupNewSchema() throws SchemaChangeException{
    this.allocationVectors = Lists.newArrayList();
    container.clear();
    final List<TransferPair> transfers = Lists.newArrayList();
    final ClassGenerator<StatsUnpivotor> cg = CodeGenerator.getRoot(StatsUnpivotor.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    final int inColCount = incoming.getSchema().getFieldCount();
    
    if(incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.NONE){
      throw new UnsupportedOperationException("Don't use a selection vectory with StatsPivot. It barely works as is.");
    }
    
    List<String> colNames = Lists.newArrayList();
    List<MajorType> colTypes = Lists.newArrayList();
    
    List<String> subrecNames = Lists.newArrayList();
    List<Integer> subrecCounts = Lists.newArrayList();
    
    int outcol [] = new int[inColCount];
    int subrec [] = new int[inColCount];
    
    // determine output destinations for each input column
    for (int i = 0; i < inColCount; i++) {
      MaterializedField inMf = incoming.getSchema().getColumn(i);
      String [] ss = inMf.getLastName().split("_", 2);
      String oc = ss.length > 1 ? ss[0] : inMf.getLastName();
      String sr = ss.length > 1 ? ss[ss.length - 1] : null;
      
      outcol[i] = lookupOrAddCol(oc, inMf.getType(), colNames, colTypes);
      subrec[i] = sr != null ? lookupOrAddSubrec(sr, subrecNames, subrecCounts) : -1;
    }
    
    for (int i = 0; i < inColCount; i++) {
      System.out.println(incoming.getSchema().getColumn(i).getLastName() + " " + outcol[i] + " " + subrec[i]);
    }
    
    // create output columns and fields
    List<MaterializedField> outFields = Lists.newArrayList();
    outFields.add(addOutputCol("column", Types.required(MinorType.VARCHAR)));
    for (int i = 0; i < colNames.size(); i++) {
      outFields.add(addOutputCol(colNames.get(i), colTypes.get(i)));
    }
    
    // each subrec should have the same # of columns
    if (Collections.min(subrecCounts) != Collections.max(subrecCounts)) {
      throw new UnsupportedOperationException("Invalid set of input columns");
    }
    
    // first column contains subrec names
    for (int i = 0; i < subrecNames.size(); i++) {
      codegenAddExpression(
          cg, makeColumnNameExpression(subrecNames.get(i), outFields.get(0)),
          subrecNames.size(), i);
    }
    
    for (int i = 0; i < inColCount; i++) {
      // an input column that is only written to one (outcol, subrec) coordinate
      if (subrec[i] != -1) {
        codegenAddExpression(
            cg, makeInToOutExpression(i, outFields.get(outcol[i] + 1)),
            subrecNames.size(), subrec[i]);
        continue;
      }
      
      // non-subrec associated input col writes itself to *all* subrecs.
      for (int j = 0; j < subrecNames.size(); j++) {
        codegenAddExpression(
            cg, makeInToOutExpression(i, outFields.get(outcol[i] + 1)),
            subrecNames.size(), j);
      }
    }
    
    cg.rotateBlock();
    cg.getEvalBlock()._return(JExpr.TRUE);

    container.buildSchema(SelectionVectorMode.NONE);
    
    try {
      this.statsPivotor = context.getImplementationClass(cg.getCodeGenerator());
      statsPivotor.setup(context, incoming, this, transfers, subrecNames.size());
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }
}
