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
package org.apache.drill.exec.ops;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.common.HashTable;
import org.apache.drill.exec.physical.impl.common.HashTableStats;
import org.apache.drill.exec.physical.impl.partitionsender.PartitionStatsBatch;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.MetricValue;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.WritableBatch;

import com.carrotsearch.hppc.IntDoubleOpenHashMap;
import com.carrotsearch.hppc.IntLongOpenHashMap;

public class OperatorStats {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OperatorStats.class);

  protected final int operatorId;
  protected final int operatorType;
  private final BufferAllocator allocator;

  private IntLongOpenHashMap longMetrics = new IntLongOpenHashMap();
  private IntDoubleOpenHashMap doubleMetrics = new IntDoubleOpenHashMap();

  public long[] recordsReceivedByInput;
  public long[] batchesReceivedByInput;
  private long[] schemaCountByInput;


  private boolean inProcessing = false;
  private boolean inSetup = false;
  private boolean inWait = false;

  protected long processingNanos;
  protected long setupNanos;
  protected long waitNanos;

  private long processingMark;
  private long setupMark;
  private long waitMark;

  private long schemas;
  
  private ArrayList<MetricHelper> metricHelpers = new ArrayList<MetricHelper>();

  public OperatorStats(PhysicalOperator operator, BufferAllocator allocator) {
    this(new OpProfileDef(operator.getOperatorId(), operator.getOperatorType(), OperatorContext.getChildCount(operator)), allocator);
  }
  
  public OperatorStats(OpProfileDef def, BufferAllocator allocator){
    this(def.getOperatorId(), def.getOperatorType(), def.getIncomingCount(), allocator);
  }

  private OperatorStats(int operatorId, int operatorType, int inputCount, BufferAllocator allocator) {
    super();
    this.allocator = allocator;
    this.operatorId = operatorId;
    this.operatorType = operatorType;
    this.recordsReceivedByInput = new long[inputCount];
    this.batchesReceivedByInput = new long[inputCount];
    this.schemaCountByInput = new long[inputCount];
  }

  private String assertionError(String msg){
    return String.format("Failure while %s for operator id %d. Currently have states of processing:%s, setup:%s, waiting:%s.", msg, operatorId, inProcessing, inSetup, inWait);
  }
  public void startSetup() {
    assert !inSetup  : assertionError("starting setup");
    stopProcessing();
    inSetup = true;
    setupMark = System.nanoTime();
  }

  public void stopSetup() {
    assert inSetup :  assertionError("stopping setup");
    startProcessing();
    setupNanos += System.nanoTime() - setupMark;
    inSetup = false;
  }

  public void startProcessing() {
    assert !inProcessing : assertionError("starting processing");
    processingMark = System.nanoTime();
    inProcessing = true;
  }

  public void stopProcessing() {
    assert inProcessing : assertionError("stopping processing");
    processingNanos += System.nanoTime() - processingMark;
    inProcessing = false;
  }

  public void startWait() {
    assert !inWait : assertionError("starting waiting");
    stopProcessing();
    inWait = true;
    waitMark = System.nanoTime();
  }

  public void stopWait() {
    assert inWait : assertionError("stopping waiting");
    startProcessing();
    waitNanos += System.nanoTime() - waitMark;
    inWait = false;
  }

  public void batchReceived(int inputIndex, long records, boolean newSchema) {
    recordsReceivedByInput[inputIndex] += records;
    batchesReceivedByInput[inputIndex]++;
    if(newSchema){
      schemaCountByInput[inputIndex]++;
    }
  }

  public OperatorProfile getProfile() {
    final OperatorProfile.Builder b = OperatorProfile //
        .newBuilder() //
        .setOperatorType(operatorType) //
        .setOperatorId(operatorId) //
        .setSetupNanos(setupNanos) //
        .setProcessNanos(processingNanos)
        .setWaitNanos(waitNanos);

    if(allocator != null){
      b.setLocalMemoryAllocated(allocator.getAllocatedMemory());
    }



    addAllMetrics(b);

    return b.build();
  }

  public void addAllMetrics(OperatorProfile.Builder builder) {
    for (MetricHelper mh : metricHelpers) {
      mh.addHelperMetrics(builder);
    }
    addStreamProfile(builder);
    addLongMetrics(builder);
    addDoubleMetrics(builder);
  }

  public void addStreamProfile(OperatorProfile.Builder builder) {
    for(int i = 0; i < recordsReceivedByInput.length; i++){
      builder.addInputProfile(StreamProfile.newBuilder().setBatches(batchesReceivedByInput[i]).setRecords(recordsReceivedByInput[i]).setSchemas(this.schemaCountByInput[i]));
    }
  }

  public void addLongMetrics(OperatorProfile.Builder builder) {
    for(int i =0; i < longMetrics.allocated.length; i++){
      if(longMetrics.allocated[i]){
        builder.addMetric(MetricValue.newBuilder().setMetricId(longMetrics.keys[i]).setLongValue(longMetrics.values[i]));
      }
    }
  }

  public void addDoubleMetrics(OperatorProfile.Builder builder) {
    for(int i =0; i < doubleMetrics.allocated.length; i++){
      if(doubleMetrics.allocated[i]){
        builder.addMetric(MetricValue.newBuilder().setMetricId(doubleMetrics.keys[i]).setDoubleValue(doubleMetrics.values[i]));
      }
    }
  }

  public void addLongStat(MetricDef metric, long value){
    longMetrics.putOrAdd(metric.metricId(), value, value);
  }

  public void addDoubleStat(MetricDef metric, double value){
    doubleMetrics.putOrAdd(metric.metricId(), value, value);
  }
  
  public void registerMetricHelper(MetricHelper mh) {
    metricHelpers.add(mh);
  }
  
  public interface MetricHelper {
    public void addHelperMetrics(OperatorProfile.Builder builder);
  }
  
  public static class OutgoingBatchMetricHelper implements MetricHelper {
    private final MetricDef minMetric;
    private final MetricDef maxMetric;
    
    private long max = Long.MIN_VALUE;
    private long min = Long.MAX_VALUE;
    
    public void addHelperMetrics(OperatorProfile.Builder builder) {
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(max).setMetricId(maxMetric.metricId()));
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(min).setMetricId(minMetric.metricId()));
    }
    
    public OutgoingBatchMetricHelper(MetricDef minMetric, MetricDef maxMetric) {
      this.minMetric = minMetric;
      this.maxMetric = maxMetric;
    }
    
    public void update(List<? extends PartitionStatsBatch> outgoing) {
      for (PartitionStatsBatch b : outgoing) {
        max = Math.max(b.getTotalRecords(), max);
        min = Math.min(b.getTotalRecords(), min);
      }
    }
    
    public void update(long n) {
      max = Math.max(n, max);
      min = Math.min(n, min);
    }
  }
  
  public static class CounterMetricHelper implements MetricHelper {
    private final MetricDef counterMetric;
    private long count = 0;
    
    public void addHelperMetrics(OperatorProfile.Builder builder) {
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(count).setMetricId(counterMetric.metricId()));
    }
    
    public CounterMetricHelper(MetricDef counterMetric) {
      this.counterMetric = counterMetric;
    }
    
    public void increment() {
      count++;
    }
  }
  
  public static class HashTableMetricHelper implements MetricHelper {
    private final HashTableStats htStats = new HashTableStats();
    private final MetricDef bucketsMetric;
    private final MetricDef entriesMetric;
    private final MetricDef resizingMetric;
    private final MetricDef resizingTimeMetric;

    public void addHelperMetrics(OperatorProfile.Builder builder) {
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(htStats.numBuckets).setMetricId(bucketsMetric.metricId()));
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(htStats.numEntries).setMetricId(entriesMetric.metricId()));
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(htStats.numResizing).setMetricId(resizingMetric.metricId()));
      builder.addMetric(UserBitShared.MetricValue.newBuilder().setLongValue(htStats.numResizingTime).setMetricId(resizingTimeMetric.metricId()));
    }
    
    public HashTableMetricHelper(MetricDef bucketsMetric, MetricDef entriesMetric, MetricDef resizingMetric, MetricDef resizingTimeMetric) {
      this.bucketsMetric = bucketsMetric;
      this.entriesMetric = entriesMetric;
      this.resizingMetric = resizingMetric;
      this.resizingTimeMetric = resizingTimeMetric;
    }
    
    public void update(HashTable htable) {
      if (htable != null) {
        htable.getStats(htStats);
      }
    }
  }

}
