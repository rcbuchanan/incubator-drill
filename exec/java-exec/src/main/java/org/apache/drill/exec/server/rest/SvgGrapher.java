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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;

public class SvgGrapher {
  public static String makeSvg(PhysicalPlan plan) {
    PhysicalOperator root = plan.getSortedOperators(false).iterator().next();
    
    class VisualizationVisitor extends AbstractPhysicalVisitor<String, String, RuntimeException> {
      @Override
      public String visitOp(PhysicalOperator op, String value) throws RuntimeException {
        value += op.getOperatorId() + " [label = \"" + op.getClass().getName() + "\"];\n";
        
        for (PhysicalOperator child : op) {
          value += op.getOperatorId() + " -> " + child.getOperatorId() + ";\n";
          value += child.accept(this, "");
        }
        return value;
      }
    }
    
    String input = "digraph G {\nnode [shape = record];\n"
        + root.accept(new VisualizationVisitor(), "") + "\n}\n";
    
    return runGraphviz(input);
  }
  
  private static String runGraphviz(String input) {

    String [] cmd = {"/usr/local/bin/dot", "-Tsvg"};
    String output = "";
    
    try {
      Process proc = Runtime.getRuntime().exec(cmd);
      PrintWriter pw = new PrintWriter(proc.getOutputStream());
      pw.write(input);
      pw.close();
      
      proc.waitFor();
      
      BufferedReader svgReader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line;
      while ((line = svgReader.readLine()) != null) {
        output += line;
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return output;
  }
}
