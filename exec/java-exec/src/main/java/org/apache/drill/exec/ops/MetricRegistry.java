package org.apache.drill.exec.ops;

import java.util.HashMap;

import org.apache.drill.exec.proto.beans.CoreOperatorType;

class MetricRegistry {
  private static MetricRegistry instance;
  private HashMap<CoreOperatorType, Class<? extends MetricDef>> metricDefs;
  
  private final static Class[] knownMetricDefs = {
      org.apache.drill.exec.physical.impl.partitionsender.PartitionSenderStats.class,
      org.apache.drill.exec.physical.impl.mergereceiver.MergingRecordBatch.Metric.class };

  static MetricRegistry getInstance() {
    if (instance == null) {
      instance = new MetricRegistry();
      buildRegistry();
    }

    return instance;
  }

  private MetricRegistry() {
  }
  
  private static void buildRegistry() {
    instance.metricDefs = new HashMap<CoreOperatorType, Class<? extends MetricDef>>();
    for (Class<? extends MetricDef> c : knownMetricDefs) {
      selfRegister(c);
    }
  }

  private static void selfRegister(Class<? extends MetricDef> c) {
    for (MetricDef m : c.getEnumConstants()) {
      for (CoreOperatorType cot : m.supported()) {
        Class<? extends MetricDef> prev = getInstance().metricDefs.put(cot, c);
        if (prev == null) {
          System.out.println("Warning: multiply specified metricDef");
        }
      }
      break;
    }
  }

  public static String lookupMetric(int optype, int metric) {
    CoreOperatorType cot = CoreOperatorType.valueOf(optype);

    if (getInstance().metricDefs.containsKey(cot)) {
      for (MetricDef m : getInstance().metricDefs.get(cot).getEnumConstants()) {
        if (m.metricId() == metric) {
          return m.name();
        }
      }
    }
    return "undefined";
  }
}