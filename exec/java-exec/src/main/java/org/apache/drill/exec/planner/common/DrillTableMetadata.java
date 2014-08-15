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
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;

public class DrillTableMetadata {
  private Table statsTable;
  private String statsTableString;
  List<Map<String, Object>> result;
  Map<String, Long> ndv;
  double rowcount = -1;
  
  public DrillTableMetadata(Table statsTable, String statsTableString) {
    // TODO: this should actually be a Table object or a selection or something instead of a string
    this.statsTable = statsTable;
    this.statsTableString = statsTableString;
  }
  
  public Double getRowCount() {
    return rowcount > 0 ? new Double(rowcount) : null;
  }
  
  public Double getDistinctRowCount(List<RelDataTypeField> fields, BitSet selection) {
    if (ndv == null) {
      return null;
    } else if (selection.cardinality() == 0) {
      System.out.println("ignoring request for NDV for empty bitset");
      return null;
    } else if (selection.cardinality() > 1) {
      System.out.println("ignoring request for joint NDV");
      return null;
    }
    
    int n = selection.length() - 1;
    
    if (n > 0 && n < fields.size()) {
      return new Double(ndv.get(fields.get(n).getName()));
    } else {
      return null;
    }
  }
  
  public void materialize(QueryContext context) throws Exception {
    if (statsTableString == null || statsTable == null)
      return;

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
    
    ndv = new HashMap<String, Long>();
    for (Map<String, Object> r : result) {
      ndv.put((String) r.get("column"), (Long) r.get("ndv"));
      rowcount = Math.max(rowcount, r.containsKey("statcount") ? (Long) r.get("statcount") : 0);
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
      if (node instanceof DrillScanRel) {
        try {
          DrillScanRel dsr = (DrillScanRel) node;
          if (dsr.getDrillTable().getDrillTableMetadata() != null) {
            dsr.getDrillTable().getDrillTableMetadata().materialize(context);
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
