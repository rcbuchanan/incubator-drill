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
                  BuiltinMethod.DISTINCT_ROW_COUNT.method, SINGLETON),
          ReflectiveRelMetadataProvider.reflectiveSource(
              BuiltinMethod.ROW_COUNT.method, SINGLETON)));
  
  private DrillScanRelMdProvider() {}
  
  public Double getRowCount(DrillScanRel rel) {
    return rel.getDrillTable().getDrillTableMetadata() == null ? null : rel.getDrillTable().getDrillTableMetadata().getRowCount();
  }

  public Double getDistinctRowCount(
      DrillScanRelBase rel,
      BitSet groupKey,
      RexNode predicate) {
    List<RelColumnOrigin> cols = Lists.newArrayList();
    
    if (groupKey.length() == 0) {
      return new Double(0);
    }
    
    if (rel.getDrillTable() == null) {
      return null;
    }
    
    DrillTableMetadata md = rel.getDrillTable().getDrillTableMetadata();
    
    if (md == null) {
      return null;
    }
    
    double rc = RelMetadataQuery.getRowCount(rel);
    double s = 1.0;
    for (int i = 0; i < groupKey.length(); i++) {
      String n = rel.getRowType().getFieldNames().get(i);
      if (!groupKey.get(i) && !n.equals("*")) {
        continue;
      }
      
      Double d = md.getNdv(n);
      if (d == null) {
        continue;
      }
      
      s *= 1 - d / rc;
    }
    return new Double((1 - s) * rc);
  }
  
  public Double getDistinctRowCount(
      ScanPrel prel,
      BitSet groupKey,
      RexNode predicate) {
    //TODO: move this code into the other thing!!!
    List<RelColumnOrigin> cols = Lists.newArrayList();
    
    if (groupKey.length() == 0) {
      return new Double(0);
    }
    
    if (prel.getDrillTable() == null) {
      return null;
    }
    
    DrillTableMetadata md = prel.getDrillTable().getDrillTableMetadata();
    
    if (md == null) {
      return null;
    }
    
    double rc = RelMetadataQuery.getRowCount(prel);
    double s = 1.0;
    for (int i = 0; i < groupKey.length(); i++) {
      String n = prel.getRowType().getFieldNames().get(i);
      if (!groupKey.get(i) && !n.equals("*")) {
        continue;
      }
      
      Double d = md.getNdv(n);
      if (d == null) {
        continue;
      }
      
      s *= 1 - d / rc;
    }
    return new Double(s * rc);
  }
  
}
