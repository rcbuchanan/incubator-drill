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
package org.apache.drill.exec.planner.sql.handlers;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.parser.SqlAnalyzeTable;
import org.apache.drill.exec.planner.types.DrillFixedRelDataTypeImpl;
import org.apache.drill.exec.store.AbstractSchema;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.parser.SqlParserPos;

import java.io.IOException;
import java.util.List;

public class AnalyzeTableHandler extends DefaultSqlHandler {

  public AnalyzeTableHandler(Planner planner, QueryContext context) {
    super(planner, context);
  }
  
  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
    SqlAnalyzeTable sqlAnalyzeTable = unwrap(sqlNode, SqlAnalyzeTable.class);
    
    try {
      SqlIdentifier tableIdentifier = sqlAnalyzeTable.getTableIdentifier();
      SqlNodeList allNodes = new SqlNodeList(SqlParserPos.ZERO);
      allNodes.add(new SqlIdentifier("*", SqlParserPos.ZERO));
      SqlSelect scanSql = new SqlSelect(
          SqlParserPos.ZERO, /* position */
          SqlNodeList.EMPTY, /* keyword list */
          allNodes, /*select list */
          tableIdentifier, /* from */
          null, /* where */
          null, /* group by */
          null, /* having */
          null, /* windowDecls */
          null, /* orderBy */
          null, /* offset */
          null /* fetch */);
      
      SqlNode rewrittenScan = rewrite(scanSql);
      SqlNode validated = validateNode(rewrittenScan);
      RelNode relScan = convertToRel(validated);

      // don't analyze all columns
//      List<String> analyzeFields = sqlAnalyzeTable.getFieldNames();
//      if (analyzeFields.size() > 0) {
//        RelDataType rowType = new DrillFixedRelDataTypeImpl(
//            planner.getTypeFactory(), analyzeFields);
//        relScan = RelOptUtil.createCastRel(relScan, rowType, true);
//      }
      
      
      SchemaPlus schema = findSchema(
          context.getRootSchema(),
          context.getNewDefaultSchema(),
          sqlAnalyzeTable.getSchemaPath());

      AbstractSchema drillSchema = getDrillSchema(schema);
      
      if (!drillSchema.isMutable())
        return DirectPlan.createDirectPlan(context, false, String.format("Current schema '%s' is not a mutable schema. " +
            "Can't create tables in this schema.", drillSchema.getFullSchemaName()));
      
      String analyzeTableName = sqlAnalyzeTable.getName();
      
      // Convert the query to Drill Logical plan and insert a writer operator on top.
      DrillRel drel = convertToDrel(relScan, drillSchema, analyzeTableName);
      log("Drill Logical", drel);
      
      Prel prel = convertToPrel(drel);
      log("Drill Physical", prel);
      PhysicalOperator pop = convertToPop(prel);
      PhysicalPlan plan = convertToPlan(pop);
      log("Drill Plan", plan);
      
      return plan;
    } catch (Exception e) {
      logger.error("Failed analyze table '{}'", sqlAnalyzeTable.getName(), e);
      return DirectPlan.createDirectPlan(context, false, String.format("Error: %s", e.getMessage()));

    }
  }

  protected DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String analyzeTableName) throws RelConversionException {
    RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_RULES,
        relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    } else {
      RelNode writerRel = new DrillWriterRel(
              convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              new DrillAnalyzeRel(
                      convertedRelNode.getCluster(),
                      convertedRelNode.getTraitSet(),
                      convertedRelNode),
              schema.appendToMetadataTable(analyzeTableName));
      return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
    }
  }
}
