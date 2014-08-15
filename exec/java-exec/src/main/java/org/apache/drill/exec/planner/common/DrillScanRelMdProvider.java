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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelColumnOrigin;
import org.eigenbase.rel.metadata.RelMdUtil;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlKind;

import com.google.common.collect.Lists;

/**
 * Thin interface around DrillTableMetadata that reads out of
 * nodes that have drillTables attached to them
 */
public class DrillScanRelMdProvider {
  private static final DrillScanRelMdProvider SINGLETON = new DrillScanRelMdProvider();
  public static final RelMetadataProvider SOURCE = ChainedRelMetadataProvider.of(
      Lists.newArrayList(
          ReflectiveRelMetadataProvider.reflectiveSource(
              BuiltinMethod.POPULATION_SIZE.method, SINGLETON),
          ReflectiveRelMetadataProvider.reflectiveSource(
              BuiltinMethod.ROW_COUNT.method, SINGLETON),
          ReflectiveRelMetadataProvider.reflectiveSource(
              BuiltinMethod.COLUMN_ORIGIN.method, SINGLETON)));
  
  private DrillScanRelMdProvider() {}
  
  public Double getRowCount(DrillScanRel rel) {
    return rel.getDrillTable().getDrillTableMetadata() == null ? null : rel.getDrillTable().getDrillTableMetadata().getRowCount();
  }
  
  public Double getRowCount(ScanPrel prel) {
    return prel.getDrillTable().getDrillTableMetadata() == null ? null : prel.getDrillTable().getDrillTableMetadata().getRowCount();
  }
  
  public Double getPopulationSize(
      RelSubset rel,
      BitSet groupKey) {
    return rel.getBest() != null ? RelMetadataQuery.getPopulationSize(rel.getBest(), groupKey) : null;
  }
  

//Catch-all rule when none of the others apply.
  public Set<RelColumnOrigin> getColumnOrigins(
     RelNode rel,
     int iOutputColumn) {
   // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table
   // expression which supports projections.  In that case,
   // it's up to the plugin writer to override with the
   // correct information.

   if (rel.getInputs().size() > 0) {
     // No generic logic available for non-leaf rels.
     return null;
   }

   Set<RelColumnOrigin> set = new HashSet<RelColumnOrigin>();

   RelOptTable table = rel.getTable();
   if (table == null) {
     // Somebody is making column values up out of thin air, like a
     // VALUES clause, so we return an empty set.
     return set;
   }

   // Detect the case where a physical table expression is performing
   // projection, and say we don't know instead of making any assumptions.
   // (Theoretically we could try to map the projection using column
   // names.)  This detection assumes the table expression doesn't handle
   // rename as well.
   if (table.getRowType() != rel.getRowType()) {
     iOutputColumn++;
   }

   set.add(new RelColumnOrigin(table, iOutputColumn, false));
   return set;
 }

  public Double getPopulationSize(
      DrillScanRel rel,
      BitSet groupKey) {
    List<RelColumnOrigin> cols = Lists.newArrayList();
    
    if (groupKey.length() == 0) {
      //System.out.println(rel);
      //System.out.println("No columns for population?");
      //return null;
      return new Double(0);
    }
    
    for (int i = 0; i < groupKey.length(); i++) {
      if (!groupKey.get(i)) {
        continue;
      }
      
      RelColumnOrigin col = RelMetadataQuery.getColumnOrigin(rel, i);
      if (col == null) {
        System.out.println("null col found in rel");
      }
      cols.add(col);
    }
    
    return rel.getDrillTable().getDrillTableMetadata() == null ? null :
        rel.getDrillTable().getDrillTableMetadata().getPopulationSize(cols, groupKey);
  }
  
  public Double getPopulationSize(
      ScanPrel prel,
      BitSet groupKey) {
    List<RelColumnOrigin> cols = Lists.newArrayList();
    
    if (groupKey.length() == 0) {
//      System.out.println("No columns for population?");
//      return null;
      return new Double(0);
    }
    
    for (int i = 0; i < groupKey.length(); i++) {
      if (!groupKey.get(i)) {
        continue;
      }
      
      RelColumnOrigin col = RelMetadataQuery.getColumnOrigin(prel, i);
      if (col == null) {
        return null;
      }
      cols.add(col);
    }
    
    return prel.getDrillTable().getDrillTableMetadata() == null ? null :
        prel.getDrillTable().getDrillTableMetadata().getPopulationSize(cols, groupKey);
  }
}
