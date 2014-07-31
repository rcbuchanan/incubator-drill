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
package org.apache.drill.exec.planner.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.AggPrelBase.OperatorPhase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexNode;

public class AnalyzePrule extends Prule {
  public static final RelOptRule INSTANCE = new AnalyzePrule();


  public AnalyzePrule() {
    super(RelOptHelper.some(DrillAnalyzeRel.class, DrillRel.DRILL_LOGICAL, RelOptHelper.any(RelNode.class)), "Prel.AnalyzePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final DrillAnalyzeRel analyze = (DrillAnalyzeRel) call.rel(0);
    final RelNode input = call.rel(1);
    
    System.out.println("MATCHES!!!");

    final RelTraitSet traits = input.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    final RelNode convertedInput = convert(input, traits);
    UnpivotPrel newAnalyze = null;
    try {
      newAnalyze = new UnpivotPrel(
          analyze.getCluster(),
          analyze.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON),
          new StatsAggPrel(
              analyze.getCluster(),
              analyze.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON),
              convertedInput,
              OperatorPhase.PHASE_1of1),
          new ArrayList<RexNode>(),
          analyze.getRowType());
    } catch (InvalidRelException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    call.transformTo(newAnalyze);
  }


}
