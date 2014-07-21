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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import net.hydromatic.linq4j.BaseQueryable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.optiq.QueryableTable;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.AbstractSchema;
import net.hydromatic.optiq.impl.ViewTable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.impl.java.ReflectiveSchema;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.QueryStatus;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelProtoDataType;
import org.eigenbase.sql.type.SqlTypeName;
import org.glassfish.jersey.server.mvc.Viewable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

@Path("/query")
public class QueryResources {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryResources.class);

  @Inject
  WorkManager work;

  @GET
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery() {
    return new Viewable("/rest/query/query.ftl");
  }

  public static class FooStruct {
    public final int blah = 10;
    public final BarStruct [] bars = {
        new BarStruct(0),
        new BarStruct(1),
        new BarStruct(3)
    };
    
    public static class BarStruct {
      public final int depth;
      public final String bonus = "hidden message";
      public final FnordStruct nest;
      
      public BarStruct(int d) {
        this.depth = d;
        this.nest = new FnordStruct();
      }
    }
    
    public static class FnordStruct {
      public final double ah = Math.random();
    }
  }
  
  @POST
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  @Produces(MediaType.TEXT_HTML)
  public String submitQuery(@FormParam("query") String query, @FormParam("queryType") String queryType) throws Exception {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
      Connection connection =
          DriverManager.getConnection("jdbc:optiq:");
      OptiqConnection optiqConnection =
          connection.unwrap(OptiqConnection.class);
      SchemaPlus rootSchema = optiqConnection.getRootSchema();
      rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
      
      ResultSet results = optiqConnection.createStatement().executeQuery(
        "select \"deptno\", cardinality(\"employees\") as c\n"
        + "from \"hr\".\"depts\""
            );
      printResultSet(results);
    } catch (Throwable e) {
      e.printStackTrace(System.out);
    }
    return "<html><body>Didn't crash??</body></html>";
  }
  
  public static QueryableTable generateStrings(final Integer count) {
    return new AbstractQueryableTable(IntString.class) {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(IntString.class);
      }
  
      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        BaseQueryable<IntString> queryable =
            new BaseQueryable<IntString>(null, IntString.class, null) {
              public Enumerator<IntString> enumerator() {
                return new Enumerator<IntString>() {
                  static final String Z = "abcdefghijklm";
  
                  int i = 0;
                  int curI;
                  String curS;
  
                  public IntString current() {
                    return new IntString(curI, curS);
                  }
  
                  public boolean moveNext() {
                    if (i < count) {
                      curI = i;
                      curS = Z.substring(0, i % Z.length());
                      ++i;
                      return true;
                    } else {
                      return false;
                    }
                  }
  
                  public void reset() {
                    i = 0;
                  }
  
                  public void close() {
                  }
                };
              }
            };
        //noinspection unchecked
        return (Queryable<T>) queryable;
      }
    };
  }

  public static TranslatableTable view(String s) {
    return new ViewTable(Object.class,
        new RelProtoDataType() {
          public RelDataType apply(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("c", SqlTypeName.INTEGER)
                .build();
          }
        }, "values (1), (3), " + s, ImmutableList.<String>of());
  }

  public void printResultSet(ResultSet results) throws SQLException {
    if (!results.next()) return;
    
    System.out.println("**** : " + results.getMetaData().getColumnCount());
    for (int i = 1; i <= results.getMetaData().getColumnCount(); i++) {
      System.out.print(results.getMetaData().getColumnName(i) + "\t");
    }
    System.out.println();
    do {
      for (int i = 1; i <= results.getMetaData().getColumnCount(); i++) {
        System.out.print(i + ":" + results.getString(i) + "\t");
      }
      System.out.println();
    } while (results.next());
  }

  public void exploreSchema(SchemaPlus s, int d) {
    for (int i = 0; i < d; i++) {
      System.out.print("  ");
    }
    
    System.out.print("\"" + s.getName() + "\" (");
    for (String fn : s.getFunctionNames()) {
      System.out.print(fn + ", ");
    }
    System.out.print(") : ");
    
    for (String tn : s.getTableNames()) {
      System.out.print(tn + ", ");
    }
    System.out.println();
    
    for (String sn : s.getSubSchemaNames()) {
      exploreSchema(s.getSubSchema(sn), d + 1);
    }
  }

  private static class Listener implements UserResultsListener {
    private volatile Exception exception;
    private AtomicInteger count = new AtomicInteger();
    private CountDownLatch latch = new CountDownLatch(1);
    private LinkedList<LinkedList<String>> output = new LinkedList<>();
    private RecordBatchLoader loader;

    Listener(RecordBatchLoader loader) {
      this.loader = loader;
    }

    @Override
    public void submissionFailed(RpcException ex) {
      exception = ex;
      System.out.println("Query failed: " + ex.getMessage());
      latch.countDown();
    }

    @Override
    public void resultArrived(QueryResultBatch result, ConnectionThrottle throttle) {
      int rows = result.getHeader().getRowCount();
      if (result.getData() != null) {
        count.addAndGet(rows);
        try {
          loader.load(result.getHeader().getDef(), result.getData());
          output.add(new LinkedList<String>());
          for (int i = 0; i < loader.getSchema().getFieldCount(); ++i) {
            output.getLast().add(loader.getSchema().getColumn(i).getPath().getAsUnescapedPath());
          }
        } catch (SchemaChangeException e) {
          throw new RuntimeException(e);
        }
        for (int i = 0; i < rows; ++i) {
          output.add(new LinkedList<String>());
          for (VectorWrapper<?> vw : loader) {
            ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
            output.getLast().add(accessor.getObject(i).toString());
          }
        }
      }
      result.release();
      if (result.getHeader().getIsLastChunk()) {
        latch.countDown();
      }
    }

    @Override
    public void queryIdArrived(UserBitShared.QueryId queryId) {
    }

    public List<LinkedList<String>> waitForCompletion() throws Exception {
      latch.await();
      if (exception != null) {
        throw exception;
      }
      return output;
    }
  }

  public static class HrSchema {
    @Override
    public String toString() {
      return "HrSchema";
    }
  
    public final Employee[] emps = {
      new Employee(100, 10, "Bill", 10000, 1000),
      new Employee(200, 20, "Eric", 8000, 500),
      new Employee(150, 10, "Sebastian", 7000, null),
      new Employee(110, 10, "Theodore", 11500, 250),
    };
    public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2])),
      new Department(30, "Marketing", Collections.<Employee>emptyList()),
      new Department(40, "HR", Collections.singletonList(emps[1])),
    };
  
    public QueryableTable foo(int count) {
      return generateStrings(count);
    }
  
    public TranslatableTable view(String s) {
      return view(s);
    }
  }

  public static class Employee {
    public final int empid;
    public final int deptno;
    public final String name;
    public final float salary;
    public final Integer commission;
  
    public Employee(int empid, int deptno, String name, float salary,
        Integer commission) {
      this.empid = empid;
      this.deptno = deptno;
      this.name = name;
      this.salary = salary;
      this.commission = commission;
    }
  
    public String toString() {
      return "Employee [empid: " + empid + ", deptno: " + deptno
          + ", name: " + name + "]";
    }
  }

  public static class Department {
    public final int deptno;
    public final String name;
    public final List<Employee> employees;
  
    public Department(
        int deptno, String name, List<Employee> employees) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
    }
  
  
    public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + "]";
    }
  }

  public static class IntString {
    public final int n;
    public final String s;
  
    public IntString(int n, String s) {
      this.n = n;
      this.s = s;
    }
  
    public String toString() {
      return "{n=" + n + ", s=" + s + "}";
    }
  }
}
