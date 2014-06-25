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

import java.util.HashMap;
import org.apache.drill.exec.proto.beans.CoreOperatorType;

public class MetricRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetricRegistry.class);

  private static MetricRegistry instance;
  private HashMap<CoreOperatorType, MetricDef> metricDefs = new HashMap<CoreOperatorType, MetricDef>();
  
  private void buildRegistry() {
    register(org.apache.drill.exec.physical.impl.broadcastsender.BroadcastSenderRootExec.Metric.class, CoreOperatorType.BROADCAST_SENDER);
    register(org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderRootExec.Metric.class, CoreOperatorType.HASH_PARTITION_SENDER);
    register(org.apache.drill.exec.physical.impl.aggregate.HashAggTemplate.Metric.class, CoreOperatorType.HASH_AGGREGATE);
    register(org.apache.drill.exec.physical.impl.join.HashJoinBatch.Metric.class, CoreOperatorType.HASH_JOIN);
    register(org.apache.drill.exec.physical.impl.limit.LimitRecordBatch.Metric.class, CoreOperatorType.LIMIT);
    register(org.apache.drill.exec.physical.impl.SingleSenderCreator.Metric.class, CoreOperatorType.SINGLE_SENDER);
    register(org.apache.drill.exec.physical.impl.mergereceiver.MergingRecordBatch.Metric.class, CoreOperatorType.MERGING_RECEIVER);
  }
  
  private void register(Class<?> c, CoreOperatorType cot) {
    Object en[] = c.getEnumConstants();
    if (en.length > 0 && en[0] instanceof MetricDef) {
      metricDefs.put(cot, (MetricDef) en[0]);
    } else {
      logger.error("could not register {}", c.getName());;
    }
  }

  public static synchronized MetricRegistry getInstance() {
    if (instance == null) {
      instance = new MetricRegistry();
      instance.buildRegistry();
    }

    return instance;
  }

  private MetricRegistry() {
  }

  public String lookupMetric(int optype, int metric) {
    CoreOperatorType cot = CoreOperatorType.valueOf(optype);
    
    if (metricDefs.containsKey(cot)) {
      for (MetricDef m : metricDefs.get(cot).getClass().getEnumConstants()) {
        if (m.metricId() == metric) {
          return m.name();
        }
      }
    }
    return String.format("undefined (id = %d)", metric);
  }
}
