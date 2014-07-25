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
package org.apache.drill.exec.physical.impl.aggregate;

import java.io.IOException;
import java.util.List;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.NullExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.StatisticsAggregate;
import org.apache.drill.exec.physical.config.StreamingAggregate;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JVar;

public class StatisticsAggBatch extends StreamingAggBatch {
  private String[] funcs;
  private int schemaId;

  public StatisticsAggBatch(StatisticsAggregate popConfig, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(popConfig, incoming, context);
    this.funcs = popConfig.getFuncs();
  }

  protected StreamingAggregator createAggregatorInternal() throws SchemaChangeException, ClassTransformationException, IOException{
    ClassGenerator<StreamingAggregator> cg = CodeGenerator.getRoot(StreamingAggTemplate.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    container.clear();

    LogicalExpression[] keyExprs = new LogicalExpression[1];
    LogicalExpression[] valueExprs = new LogicalExpression[incoming.getSchema().getFieldCount() * funcs.length];
    TypedFieldId[] keyOutputIds = new TypedFieldId[1];

    ErrorCollector collector = new ErrorCollectorImpl();

    {
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(
          new ValueExpressions.IntExpression(schemaId++, ExpressionPosition.UNKNOWN),
          incoming, collector, context.getFunctionRegistry() );
      keyExprs[0] = expr;
      final MaterializedField outputField = MaterializedField.create("schemaId", expr.getMajorType());
      ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
      keyOutputIds[0] = container.add(vector);
    }

    for (int i = 0; i < funcs.length; i++) {
      for(int j = 0; j < incoming.getSchema().getFieldCount(); j++){
        List<LogicalExpression> args = Lists.newArrayList();
        args.add(incoming.getSchema().getColumn(j).getPath());
        int fieldno = i * incoming.getSchema().getFieldCount() + j;
        
        LogicalExpression expr = ExpressionTreeMaterializer.materialize(
            FunctionCallFactory.createExpression(funcs[i], args),
            incoming, collector, context.getFunctionRegistry());
        if(expr == null) {
          expr = new NullExpression();
        }
        
        final MaterializedField outputField = MaterializedField.create(
            funcs[i] + "_" + incoming.getSchema().getColumn(j).getLastName(),
            expr.getMajorType());
        ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
        TypedFieldId id = container.add(vector);
        valueExprs[fieldno] = new ValueVectorWriteExpression(id, expr, true);
        
      }
    }

    if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());

    setupIsSame(cg, keyExprs);
    setupIsSameApart(cg, keyExprs);
    addRecordValues(cg, valueExprs);
    outputRecordKeys(cg, keyOutputIds, keyExprs);
    outputRecordKeysPrev(cg, keyOutputIds, keyExprs);

    cg.getBlock("resetValues")._return(JExpr.TRUE);
    getIndex(cg);

    container.buildSchema(SelectionVectorMode.NONE);
    StreamingAggregator agg = context.getImplementationClass(cg);
    agg.setup(context, incoming, this);
    return agg;
  }
}
