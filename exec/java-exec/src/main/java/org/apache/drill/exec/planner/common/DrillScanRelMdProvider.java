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
import java.util.List;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;

/**
 * Thin interface around DrillTableMetadata that reads out of
 * nodes that have drillTables attached to them
 */
public class DrillScanRelMdProvider {
  public static final RelMetadataProvider SOURCE = 
  ReflectiveRelMetadataProvider.reflectiveSource(
      BuiltinMethod.DISTINCT_ROW_COUNT.method, new DrillScanRelMdProvider());
  
  private DrillScanRelMdProvider() {}
  
  public Double getDistinctRowCount(
      DrillScanRel rel,
      BitSet groupKey,
      RexNode predicate) {
    List<RelDataTypeField> fields = rel.getDrillTable().getRowType(rel.getCluster().getTypeFactory()).getFieldList();
    return rel.getDrillTable().getDrillTableMetadata() == null ? null :
        rel.getDrillTable().getDrillTableMetadata().getDistinctRowCount(fields, groupKey);
  }
  
  public Double getDistinctRowCount(
      ScanPrel prel,
      BitSet groupKey,
      RexNode predicate) {
    List<RelDataTypeField> fields = prel.getDrillTable().getRowType(prel.getCluster().getTypeFactory()).getFieldList();
    return prel.getDrillTable().getDrillTableMetadata() == null ? null :
        prel.getDrillTable().getDrillTableMetadata().getDistinctRowCount(fields, groupKey);
  }
}
