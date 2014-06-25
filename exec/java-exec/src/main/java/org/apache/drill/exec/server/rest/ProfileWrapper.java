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
package org.apache.drill.exec.server.rest;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.exec.ops.MetricRegistry;
import org.apache.drill.exec.proto.UserBitShared.MajorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.MetricValue;
import org.apache.drill.exec.proto.UserBitShared.MinorFragmentProfile;
import org.apache.drill.exec.proto.UserBitShared.OperatorProfile;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.StreamProfile;
import org.apache.drill.exec.proto.beans.CoreOperatorType;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.TreeMap;

public class ProfileWrapper {
  public QueryProfile profile;

  public ProfileWrapper(QueryProfile profile) {
    this.profile = profile;
  }

  public QueryProfile getProfile() {
    return profile;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    ArrayList<MajorFragmentProfile> majors = new ArrayList<MajorFragmentProfile>(profile.getFragmentProfileList());
    
    Collections.sort(majors, Comparators.majorIdCompare);
    builder.append(queryTimingProfile(majors));
    for (MajorFragmentProfile m : majors) {
      builder.append(majorFragmentOperatorProfile(m));
    }
    for (MajorFragmentProfile m : majors) {
      builder.append(majorFragmentTimingProfile(m));
    }
    for (MajorFragmentProfile m : majors) {
      for (MinorFragmentProfile mi : m.getMinorFragmentProfileList()) {
        builder.append(minorFragmentOperatorProfile(m.getMajorFragmentId(), mi));
      }
    }
    for (MajorFragmentProfile m : majors) {
      builder.append(majorFragmentMetricsProfile(m));
    }
    return builder.toString();
  }

  public String queryTimingProfile(ArrayList<MajorFragmentProfile> majors) {
    final String[] columns = {"id", "minors", "first start", "last start", "first end", "last end", "tmin", "tavg", "tmax"};
    TableBuilder builder = new TableBuilder("Query Timing Profile", "QueryTimingProfile", columns);

    
    long t0 = 0;
    for (MajorFragmentProfile m : majors) {
      ArrayList<MinorFragmentProfile> minors = new ArrayList<MinorFragmentProfile>(m.getMinorFragmentProfileList());
      final String fmt = " (<a href=\"#MinorFragment" + m.getMajorFragmentId() + "_%1$dOperatorProfile\">%1$d</a>)";
      int li = minors.size() - 1;
      double total = 0;
      
      for (MinorFragmentProfile p : minors) {
        total += p.getEndTime() - p.getStartTime();
      }
      
      builder.appendInteger(m.getMajorFragmentId(), null);
      builder.appendInteger(minors.size(), null);
      
      Collections.sort(minors, Comparators.startTimeCompare);
      if (t0 == 0) {
        t0 = minors.get(0).getStartTime();
      }
      builder.appendMillis(minors.get(0).getStartTime() - t0, String.format(fmt, minors.get(0).getMinorFragmentId()));
      builder.appendMillis(minors.get(li).getStartTime() - t0,String.format(fmt, minors.get(li).getMinorFragmentId()));

      Collections.sort(minors, Comparators.endTimeCompare);
      builder.appendMillis(minors.get(0).getEndTime() - t0,String.format(fmt, minors.get(0).getMinorFragmentId()));
      builder.appendMillis(minors.get(li).getEndTime() - t0, String.format(fmt, minors.get(li).getMinorFragmentId()));
      
      Collections.sort(minors, Comparators.runTimeCompare);
      builder.appendMillis(minors.get(0).getEndTime() - minors.get(0).getStartTime(), String.format(fmt, minors.get(0).getMinorFragmentId()));
      builder.appendMillis((long) (total / minors.size()), null);
      builder.appendMillis(minors.get(li).getEndTime() - minors.get(li).getStartTime(), String.format(fmt, minors.get(li).getMinorFragmentId()));
    }
    return builder.toString();
  }

  public String majorFragmentTimingProfile(MajorFragmentProfile majorFragmentProfile) {
    ArrayList<MinorFragmentProfile> minors = new ArrayList<MinorFragmentProfile>(majorFragmentProfile.getMinorFragmentProfileList());
    
    final String[] columns = {"id", "start", "end", "total time", "max records", "max batches"};
    TableBuilder builder = new TableBuilder(
        "Major Fragment #" + majorFragmentProfile.getMajorFragmentId() + " Timing Profile",
        "MajorFragment" + majorFragmentProfile.getMajorFragmentId() + "TimingProfile",
        columns);

    Collections.sort(minors, Comparators.minorIdCompare);
    for (MinorFragmentProfile m : minors) {
      ArrayList<OperatorProfile> ops = new ArrayList<OperatorProfile>(m.getOperatorProfileList());
      long biggestIncomingRecords = 0;
      long biggestBatches = 0;

      for (OperatorProfile op : ops) {
        long incomingRecords = 0;
        long batches = 0;
        for (StreamProfile sp : op.getInputProfileList()) {
          incomingRecords += sp.getRecords();
          batches += sp.getBatches();
        }
        biggestIncomingRecords = Math.max(biggestIncomingRecords, incomingRecords);
        biggestBatches = Math.max(biggestBatches, batches);
      }
      
      builder.appendInteger(m.getMinorFragmentId(), null);
      builder.appendTime(m.getStartTime(), null);
      builder.appendTime(m.getEndTime(), null);
      builder.appendMillis(m.getEndTime() - m.getStartTime(), null);
      
      Collections.sort(ops, Comparators.incomingRecordCompare);
      builder.appendInteger(biggestIncomingRecords, null);
      builder.appendInteger(biggestBatches, null);
    }
    return builder.toString();
  }

  public String majorFragmentOperatorProfile(MajorFragmentProfile major) {
    TreeMap<Integer, ArrayList<Pair<OperatorProfile, Integer>>> opmap =
        new TreeMap<Integer, ArrayList<Pair<OperatorProfile, Integer>>>();

    
    
    final String [] columns = {"id", "type", "setup min", "setup avg", "setup max", "process min", "process avg", "process max", "wait min", "wait avg", "wait max"};
    TableBuilder builder = new TableBuilder(
        String.format("Major Fragment #%d Operator Profile", major.getMajorFragmentId()),
        String.format("MajorFragment%dOperatorProfile", major.getMajorFragmentId()),
        columns);
    
    
    for (MinorFragmentProfile m : major.getMinorFragmentProfileList()) {
      int mid = m.getMinorFragmentId();
      
      for (OperatorProfile op : m.getOperatorProfileList()) {
        int opid = op.getOperatorId();
        
        if (!opmap.containsKey(opid)) {
          opmap.put(opid, new ArrayList<Pair<OperatorProfile, Integer>>());
        }
        opmap.get(opid).add(new ImmutablePair<OperatorProfile, Integer>(op, mid));
      }
    }
    
    for (Integer opid : opmap.keySet()) {
      ArrayList<Pair<OperatorProfile, Integer>> oplist = opmap.get(opid);
      final String fmt = " (<a href=\"#MinorFragment" + major.getMajorFragmentId() + "_%1$dOperatorProfile\">%1$d</a>)";
      int li = oplist.size() - 1;
      double totalsetup = 0;
      double totalprocess = 0;
      double totalwait = 0;

      for (Pair<OperatorProfile, Integer> opint : oplist) {
        totalsetup += opint.getLeft().getSetupNanos();
        totalprocess += opint.getLeft().getProcessNanos();
        totalwait += opint.getLeft().getWaitNanos();
      }
      
      builder.appendInteger(oplist.get(0).getLeft().getOperatorId(), null);
      builder.appendCell(CoreOperatorType.valueOf(oplist.get(0).getLeft().getOperatorType()).toString(), null);
      
      Collections.sort(oplist, Comparators.setupTimeSort);
      builder.appendNanos(oplist.get(0).getLeft().getSetupNanos(), String.format(fmt, oplist.get(0).getRight()));
      builder.appendNanos((long) (totalsetup / oplist.size()), null);
      builder.appendNanos(oplist.get(li).getLeft().getSetupNanos(), String.format(fmt, oplist.get(li).getRight()));

      Collections.sort(opmap.get(opid), Comparators.processTimeSort);
      builder.appendNanos(oplist.get(0).getLeft().getProcessNanos(), String.format(fmt, oplist.get(0).getRight()));
      builder.appendNanos((long) (totalprocess / oplist.size()), null);
      builder.appendNanos(oplist.get(li).getLeft().getProcessNanos(), String.format(fmt, oplist.get(li).getRight()));
      
      Collections.sort(opmap.get(opid), Comparators.waitTimeSort);
      builder.appendNanos(oplist.get(0).getLeft().getWaitNanos(), String.format(fmt, oplist.get(0).getRight()));
      builder.appendNanos((long) (totalwait / oplist.size()), null);
      builder.appendNanos(oplist.get(li).getLeft().getWaitNanos(), String.format(fmt, oplist.get(li).getRight()));
    }
    return builder.toString();
  }
  
  public String majorFragmentMetricsProfile(MajorFragmentProfile major) {
    TreeMap<Integer, ArrayList<Pair<OperatorProfile, Integer>>> opmap =
        new TreeMap<Integer, ArrayList<Pair<OperatorProfile, Integer>>>();

    
    
    final String [] columns = {"id", "operator", "metric", "value"};
    TableBuilder builder = new TableBuilder(
        String.format("Major Fragment #%d Metrics Profile", major.getMajorFragmentId()),
        String.format("MajorFragment%dMetricsProfile", major.getMajorFragmentId()),
        columns);
    
    
    ArrayList<MinorFragmentProfile> minors = new ArrayList<MinorFragmentProfile>(
        major.getMinorFragmentProfileList());
    Collections.sort(minors, Comparators.minorIdCompare);
    for (MinorFragmentProfile m : minors) {
      int mid = m.getMinorFragmentId();
      
      for (OperatorProfile op : m.getOperatorProfileList()) {
        int opid = op.getOperatorId();
        
        if (!opmap.containsKey(opid)) {
          opmap.put(opid, new ArrayList<Pair<OperatorProfile, Integer>>());
        }
        opmap.get(opid).add(new ImmutablePair<OperatorProfile, Integer>(op, mid));
      }
    }
    
    for (Integer opid : opmap.keySet()) {
      ArrayList<Pair<OperatorProfile, Integer>> oplist = opmap.get(opid);
      
      
      for (Pair<OperatorProfile, Integer> opint : oplist) {
        for (MetricValue metric : opint.getLeft().getMetricList()) {
          builder.appendCell(String.format("%d-%d-%d",
              major.getMajorFragmentId(),
              opint.getRight(),
              opint.getLeft().getOperatorId()), null);
          builder.appendCell(CoreOperatorType.valueOf(oplist.get(0).getLeft().getOperatorType()).toString(), null);
          builder.appendCell(MetricRegistry.getInstance().lookupMetric(opint.getLeft().getOperatorType(), metric.getMetricId()), null);
          builder.appendInteger(metric.getLongValue(), null);
        }
      }
      
      Collections.sort(oplist, Comparators.setupTimeSort);
      
    }
    return builder.toString();
  }
  
  public String minorFragmentOperatorProfile(int majorId, MinorFragmentProfile minorFragmentProfile) {
    ArrayList<OperatorProfile> oplist = new ArrayList<OperatorProfile>(minorFragmentProfile.getOperatorProfileList());
    
    final String[] columns = {"id", "type", "setup", "process", "wait", "metrics"};
    TableBuilder builder = new TableBuilder(
        String.format("Minor Fragment #%d-%d Operator Profile", majorId, minorFragmentProfile.getMinorFragmentId()),
        String.format("MinorFragment%d_%dOperatorProfile", majorId, minorFragmentProfile.getMinorFragmentId()),
        columns);

    Collections.sort(oplist, Comparators.operatorIdCompare);
    for (OperatorProfile op : oplist) {
      builder.appendInteger(op.getOperatorId(), null);
      builder.appendCell(CoreOperatorType.valueOf(op.getOperatorType()).toString(), null);
      builder.appendNanos(op.getSetupNanos(), null);
      builder.appendNanos(op.getProcessNanos(), null);
      builder.appendNanos(op.getWaitNanos(), null);
      
      String s = "";
      for (MetricValue mv : op.getMetricList()) {
        s += MetricRegistry.getInstance().lookupMetric(op.getOperatorType(), mv.getMetricId());
        s += " = " + Long.toString(mv.getLongValue()) + ", ";
      }
      builder.appendCell(s, null);
    }
    
    return builder.toString();
  }

  private static class Comparators {
    final static Comparator<MajorFragmentProfile> majorIdCompare = new Comparator<MajorFragmentProfile>() {
      public int compare(MajorFragmentProfile o1, MajorFragmentProfile o2) {
        return o1.getMajorFragmentId() < o2.getMajorFragmentId() ? -1 : 1;
      }
    };
    
    final static Comparator<MinorFragmentProfile> minorIdCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return o1.getMinorFragmentId() < o2.getMinorFragmentId() ? -1 : 1;
      }
    };
    
    final static Comparator<MinorFragmentProfile> startTimeCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return o1.getStartTime() < o2.getStartTime() ? -1 : 1;
      }
    };

    final static Comparator<MinorFragmentProfile> endTimeCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return o1.getEndTime() < o2.getEndTime() ? -1 : 1;
      }
    };

    final static Comparator<MinorFragmentProfile> runTimeCompare = new Comparator<MinorFragmentProfile>() {
      public int compare(MinorFragmentProfile o1, MinorFragmentProfile o2) {
        return o1.getEndTime() - o1.getStartTime() < o2.getEndTime() - o2.getStartTime() ? -1 : 1;
      }
    };
    
    final static Comparator<OperatorProfile> operatorIdCompare = new Comparator<OperatorProfile>() {
      public int compare(OperatorProfile o1, OperatorProfile o2) {
        return o1.getOperatorId() < o2.getOperatorId() ? -1 : 1;
      }
    };
    
    final static Comparator<OperatorProfile> incomingRecordCompare = new Comparator<OperatorProfile>() {
      public long incomingRecordCount(OperatorProfile op) {
        long count = 0;
        for (StreamProfile sp : op.getInputProfileList()) {
          count += sp.getRecords();
        }
        return count;
      }
      
      public int compare(OperatorProfile o1, OperatorProfile o2) {
        return incomingRecordCount(o1) > incomingRecordCount(o2) ? -1 : 1;
      }
    };
    
    final static Comparator<Pair<OperatorProfile, Integer>> setupTimeSort = new Comparator<Pair<OperatorProfile, Integer>>() {
      public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
        return o1.getLeft().getSetupNanos() < o2.getLeft().getSetupNanos() ? -1 : 1;
      }
    };
    
    final static Comparator<Pair<OperatorProfile, Integer>> processTimeSort = new Comparator<Pair<OperatorProfile, Integer>>() {
      public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
        return o1.getLeft().getProcessNanos() < o2.getLeft().getProcessNanos() ? -1 : 1;
      }
    };
    
    final static Comparator<Pair<OperatorProfile, Integer>> waitTimeSort = new Comparator<Pair<OperatorProfile, Integer>>() {
      public int compare(Pair<OperatorProfile, Integer> o1, Pair<OperatorProfile, Integer> o2) {
        return o1.getLeft().getWaitNanos() < o2.getLeft().getWaitNanos() ? -1 : 1;
      }
    };
  }
  
  class TableBuilder {
    NumberFormat format = NumberFormat.getInstance(Locale.US);
    DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    
    StringBuilder sb;
    int w = 0;
    int width;
    
    public TableBuilder(String title, String id, String[] columns) {
      sb = new StringBuilder();
      width = columns.length;
      
      format.setMaximumFractionDigits(3);
      format.setMinimumFractionDigits(3);
      
      sb.append(String.format("<h3 id=\"%s\">%s</h3>\n", id, title));
      sb.append("<table class=\"table table-bordered text-right\">\n<tr>");
      for (String cn : columns) {
        sb.append("<th>" + cn + "</th>");
      }
      sb.append("</tr>\n");
    }
    
    public void appendCell(String s, String link) {
      if (w == 0) {
        sb.append("<tr>");
      }
      sb.append(String.format("<td>%s%s</td>", s, link != null ? link : ""));
      if (++w >= width) {
        sb.append("</tr>\n");
        w = 0;
      }
    }
    
    public void appendTime(long d, String link) {
      appendCell(dateFormat.format(d), link);
    }
    
    public void appendMillis(long p, String link) {
      appendCell(format.format(p / 1000.0), link);
    }
    
    public void appendNanos(long p, String link) {
      appendMillis((long) (p / 1000.0 / 1000.0), link);
    }
    
    public void appendFormattedNumber(Number n, String link) {
      appendCell(format.format(n), link);
    }

    public void appendInteger(long l, String link) {
      appendCell(Long.toString(l), link);
    }
    
    public String toString() {
      String rv;
      rv = sb.append("\n</table>").toString();
      sb = null;
      return rv;
    }
  }
}
