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
package org.apache.drill.exec.physical.config;

import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("statistics-aggregate")
public class StatisticsAggregate extends AbstractSingle {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StatisticsAggregate.class);
  private final String[] funcs;

  private final float cardinality;

  @JsonCreator
  public StatisticsAggregate(@JsonProperty("child") PhysicalOperator child, @JsonProperty("funcs") String[] funcs, @JsonProperty("cardinality") float cardinality) {
    super(child);
    this.funcs = funcs;
    this.cardinality = cardinality;
  }
  
  public String[] getFuncs() {
    return funcs;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E{
    return physicalVisitor.visitStatisticsAggregate(this, value);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new StatisticsAggregate(child, funcs, cardinality);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.STREAMING_AGGREGATE_VALUE;
  }

}
