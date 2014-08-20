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
import java.util.Set;

import net.hydromatic.optiq.BuiltinMethod;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillScanRel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelVisitor;
import org.eigenbase.rel.metadata.BuiltInMetadata;
import org.eigenbase.rel.metadata.ChainedRelMetadataProvider;
import org.eigenbase.rel.metadata.Metadata;
import org.eigenbase.rel.metadata.ReflectiveRelMetadataProvider;
import org.eigenbase.rel.metadata.RelColumnOrigin;
import org.eigenbase.rel.metadata.RelMetadataProvider;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlExplainLevel;

import com.google.common.collect.Lists;


public class DrillRelMdProviderSpy {
  private static final DrillRelMdProviderSpy SINGLETON = new DrillRelMdProviderSpy();
  public static final RelMetadataProvider SOURCE;
  static {
    List<RelMetadataProvider> chain = Lists.newArrayList();
    
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.SELECTIVITY.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.UNIQUE_KEYS.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.COLUMN_UNIQUENESS.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.ROW_COUNT.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.DISTINCT_ROW_COUNT.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.PERCENTAGE_ORIGINAL_ROWS.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.POPULATION_SIZE.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.COLUMN_ORIGIN.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.CUMULATIVE_COST.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.NON_CUMULATIVE_COST.method, SINGLETON));
    chain.add(ReflectiveRelMetadataProvider.reflectiveSource(
        BuiltinMethod.EXPLAIN_VISIBILITY.method, SINGLETON));
    
    SOURCE = ChainedRelMetadataProvider.of(chain);
  }
  
  private Thread accessor = null;
  private List<String> log = null;
  
  private DrillRelMdProviderSpy() {}

  private void waitlog(String msg) {
    if (this.accessor == null) {
    } else if (this.accessor != Thread.currentThread()) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } else {
      log.add(msg);
    }
  }
  
  public static void begin() {
    SINGLETON._begin();
  }
  
  public synchronized void _begin() {
    if (this.accessor != null) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    this.accessor = Thread.currentThread();
    this.log = Lists.newArrayList();
  }
  
  public static List<String> end() {
    return SINGLETON._end();
  }
  
  private synchronized List<String> _end() {
    List<String> rv = this.log;
    
    assert this.accessor == Thread.currentThread();
    
    this.accessor = null;
    this.log = null;
    
    this.notifyAll();
    return rv;
  }
  
  public synchronized Double getSelectivity(RelNode rel, RexNode predicate) {
    waitlog("SELECTIVITY: " + rel + ", " + predicate); 
    return null;
  }
  public synchronized Set<BitSet> getUniqueKeys(RelNode rel, boolean ignoreNulls) {
    waitlog("UNIQUE KEYS: " + rel + ", " + ignoreNulls); 
    return null;
  }
  public synchronized Boolean areColumnsUnique(RelNode rel, BitSet columns, boolean ignoreNulls) {
    waitlog("ARE UNIQUE: " + rel + ", " + columns + ", " + ignoreNulls);
    return null;
  }
  public synchronized Double getRowCount(RelNode rel) {
    waitlog("ROW COUNT: " + rel); 
    return null;
  }
  public synchronized Double getDistinctRowCount(RelNode rel, BitSet groupKey, RexNode predicate) {
    waitlog("DISTINCT RC: " + rel + ", " + groupKey + ", " + predicate); 
    return null;
  }
  public synchronized Double getPercentageOriginalRows(RelNode rel) {
    waitlog("PCT ORIG ROWS: " + rel); 
    return null;
  }
  public synchronized Double getPopulationSize(RelNode rel, BitSet groupKey) {
    waitlog("POP SIZE: " + rel + ", " + groupKey); 
    return null;
  }
  public synchronized Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int outputColumn) {
    waitlog("COLUMN ORIGIN: " + rel + ", " + outputColumn); 
    return null;
  }
  public synchronized RelOptCost getCumulativeCost(RelNode rel) {
    waitlog("CUMUL COST: " + rel);
    return null;
  }
  public synchronized RelOptCost getNonCumulativeCost(RelNode rel) {
    waitlog("NON CUMUL COST: " + rel);
    return null;
  }
  public synchronized Boolean isVisibleInExplain(RelNode rel, SqlExplainLevel explainLevel) {
    return null;
  }
  
  public static class MdProviderSpyVisitor extends RelVisitor {
    public MdProviderSpyVisitor() {
    }
    
    private void printstuff(String title, List<String> stuff) {
      System.out.println(title);
      for (String s : stuff) {
        System.out.println(s);
      }
      System.out.println();
      
    }
    
    @Override
    public void visit(
        RelNode node,
        int ordinal,
        RelNode parent) {
      System.out.println("****** " + node + " (" + node.getId() + ") ******");

      DrillRelMdProviderSpy.begin();
      Double rc = RelMetadataQuery.getRowCount(node);
      printstuff("* ROW COUNT = " + rc, DrillRelMdProviderSpy.end());
      
      DrillRelMdProviderSpy.begin();
      Double sel = RelMetadataQuery.getSelectivity(node, null);
      printstuff("* SELECTIVITY = " + sel, DrillRelMdProviderSpy.end());
      
      for (int i = 0; i < node.getRowType().getFieldCount(); i++) {
        BitSet bs = new BitSet();
        bs.set(i);
        DrillRelMdProviderSpy.begin();
        Double ndv = RelMetadataQuery.getDistinctRowCount(node, bs, null);
        printstuff("* DISTINCT ROW COUNT " + i + " (\"" + node.getRowType().getFieldNames().get(i) + "\") = " + ndv, DrillRelMdProviderSpy.end());
      }
      
      System.out.println();
      System.out.println();
      
      super.visit(node, ordinal, parent);
    }
    
  }
}
