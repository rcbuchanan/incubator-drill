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
package org.apache.drill.exec.planner.logical;

import java.util.List;

import org.apache.drill.common.logical.data.Analyze;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.planner.torel.ConversionContext;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class DrillAnalyzeRel extends SingleRel implements DrillRel {

  public DrillAnalyzeRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode child) {
    super(cluster, traits, child);
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(this);
    double dCpu = RelMetadataQuery.getRowCount(getChild());
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }
  
  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillAnalyzeRel(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public LogicalOperator implement(DrillImplementor implementor) {
    //LogicalOperator inputOp = implementor.visitChild(this, 0, getChild());
    Analyze.Builder builder = Analyze.builder();
    return builder.build();
  }
  
  public static DrillUnionRel convert(Analyze analyze, ConversionContext context) throws InvalidRelException{
    throw new UnsupportedOperationException();
  }


}
