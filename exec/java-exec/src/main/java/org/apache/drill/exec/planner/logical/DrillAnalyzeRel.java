package org.apache.drill.exec.planner.logical;

import org.apache.drill.common.logical.data.Analyze;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.*;

public class DrillAnalyzeRel extends SingleRel implements DrillRel {

  protected DrillAnalyzeRel(RelOptCluster cluster, RelTraitSet traits,
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
  public LogicalOperator implement(DrillImplementor implementor) {
    //LogicalOperator inputOp = implementor.visitChild(this, 0, getChild());
    Analyze.Builder builder = Analyze.builder();
    return builder.build();
  }

}
