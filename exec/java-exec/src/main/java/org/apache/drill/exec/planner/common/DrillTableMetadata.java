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
package org.apache.drill.exec.planner.common;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.hydromatic.optiq.Table;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.QueryWrapper;
import org.apache.drill.exec.server.rest.QueryWrapper.Listener;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.metadata.RelColumnOrigin;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;

public class DrillTableMetadata {
  private Table statsTable;
  private String statsTableString;
  List<Map<String, Object>> result;
  Map<String, Long> ndv = new HashMap<String, Long>();
  Map<String, Long> count;
  double rowcount = -1;
  boolean materialized = false;
  
  public DrillTableMetadata(Table statsTable, String statsTableString) {
    // TODO: this should actually be a Table object or a selection or something instead of a string
    this.statsTable = statsTable;
    this.statsTableString = statsTableString;
  }
  
  public Double getPopulationSize(List<RelColumnOrigin> cols, BitSet groupKey) {
    if (cols.size() == 0) {
      System.out.println("Something bad happened.");
      return null;
    }
    
    List<RelDataTypeField> fl = cols.get(0).getOriginTable().getRowType().getFieldList();
    double guess = 0.0;
    
    for (int i = 0; i < cols.size(); i++) {
      String col = fl.get(cols.get(0).getOriginColumnOrdinal()).getName().toUpperCase();

      guess += 1 / ((double) ndv.get(col));
    }
    
    guess /= cols.size();
    guess = 1 / guess;
    
    return new Double(guess);
    
//    String col = cols.get(0).getOriginTable().getRowType().getFieldList().get(cols.get(0).getOriginColumnOrdinal()).getName().toUpperCase();
//    
//    System.out.println(col + " " + ndv.containsKey(col) + " " + ndv.size());
//    return ndv.containsKey(col) ? new Double(ndv.get(col)) : null;
  }
  
  public Double getRowCount() {
    return rowcount > 0 ? new Double(rowcount) : null;
  }
  
  public void materialize(QueryContext context) throws Exception {
    if (materialized) {
      return;
    } else if (statsTableString == null || statsTable == null) {
      return;
    }
    
    materialized = true;

    String sql = "select a.* from " + statsTableString +
            " as a natural inner join" +
            "(select `column`, max(`computed`) as `computed`" +
            "from " + statsTableString + " group by `column`) as b";
    System.out.println(sql);
    
    DrillbitContext dc = context.getDrillbitContext();
    DrillClient client = new DrillClient(
        dc.getConfig(),
        dc.getClusterCoordinator(),
        dc.getAllocator());
    Listener listener = new Listener(new RecordBatchLoader(dc.getAllocator()));
    
    client.connect();
    client.runQuery(UserBitShared.QueryType.SQL, sql, listener);
    
    result = listener.waitForCompletion();
    client.close();
    
    for (Map<String, Object> r : result) {
      System.out.println("PUTTING " + ((String) r.get("column")).toUpperCase());
      ndv.put(((String) r.get("column")).toUpperCase(), (Long) r.get("ndv"));
      rowcount = Math.max(rowcount, (Long) r.get("statcount"));
    }
    System.out.println("build hash map!");
  }

  /**
   * materialize on nodes that have an attached metadata object
   */
  public static class MaterializationVisitor extends RelVisitor {
    private QueryContext context;

    public MaterializationVisitor(QueryContext context) {
      this.context = context;
    }
    
    @Override
    public void visit(
        RelNode node,
        int ordinal,
        RelNode parent) {
      if (node instanceof TableAccessRelBase) {
        try {
          DrillTable dt = ((TableAccessRelBase) node).getTable().unwrap(DrillTable.class);
          if (dt.getDrillTableMetadata() != null) {
            dt.getDrillTableMetadata().materialize(context);
          }
        } catch (Exception e) {
          System.out.println("Materialization failed!!");
          e.printStackTrace();
        }
      }
      super.visit(node, ordinal, parent);
    }
    
  }
}
