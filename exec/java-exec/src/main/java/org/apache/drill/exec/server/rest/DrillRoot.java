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
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.ProtobufDrillSerializable.CQueryProfile;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.work.WorkManager;
import org.glassfish.jersey.server.mvc.Viewable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

@Path("/")
public class DrillRoot {
  public static HashMap<String, PhysicalPlan> planmap = new HashMap<String, PhysicalPlan>();
  
  static String svg = "";

  @Inject WorkManager work;

  @GET
  @Path("status")
  @Produces("text/plain")
  public String getHello() {
    return "running";
  }
  
  @GET
  @Path("svg/{queryid}")
  @Produces("image/svg+xml")
  public String getSvg(@PathParam("queryid") String queryId) {
    if (planmap.containsKey(queryId)) {
      return SvgGrapher.makeSvg(planmap.get(queryId));
    } else {
      return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"
          + "<!DOCTYPE svg PUBLIC \"-//W3C//DTD SVG 1.1//EN\" \"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd\">";
    }
  }

  @GET
  @Path("queries")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQueries() {
    DistributedMap<CQueryProfile> profiles = work.getContext().getCache()
        .getNamedMap("sys.queries", CQueryProfile.class);

    List<String> ids = Lists.newArrayList();
    for (Map.Entry<String, CQueryProfile> entry : profiles) {
      ids.add(entry.getKey());
    }

    return new Viewable("/rest/status/list.ftl", ids);
  }

  // place to POST a physical plan to associate w/ a queryid
  @GET
  @Path("/visual/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable showVisual(@PathParam("queryid") String queryId) {
    return new Viewable("/rest/visual.ftl", queryId);
  }

  // place to POST a physical plan to associate w/ a queryid
  @GET
  @Path("/submitplan/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable showPlanForm(@PathParam("queryid") String queryId) {
    return new Viewable("/rest/status/form.ftl", queryId);
  }

  @GET
  @Path("/query/{queryid}")
  @Produces(MediaType.TEXT_HTML)
  public Viewable getQuery(@PathParam("queryid") String queryId) {
    DistributedMap<CQueryProfile> profiles = work.getContext().getCache().getNamedMap("sys.queries", CQueryProfile.class);
    CQueryProfile c = profiles.get(queryId);
    QueryProfile q = c == null ? QueryProfile.getDefaultInstance() : c.getObj();

    return new Viewable("/rest/status/profile.ftl", q);

  }
  
  
  @POST
  @Path("/submitplan/{queryid}")
  @Consumes("application/x-www-form-urlencoded")
  public Response postPhysicalPlan(@PathParam("queryid") String queryId, @FormParam("plan") String planstring) {
    PhysicalPlan plan = null;
    try {
      plan = work.getContext().getPlanReader().readPhysicalPlan(planstring);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    planmap.put(queryId, plan);
    
    Response r;
    r = Response.noContent().build();

    return r;
  }
  
  /*
   * Returns query status
   */
  @GET
  @Path("/v1/query/{queryid}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getQueryJson(@PathParam("queryid") String queryId) {
    DistributedMap<CQueryProfile> profiles = work.getContext().getCache().getNamedMap("sys.queries", CQueryProfile.class);
    CQueryProfile c = profiles.get(queryId);
    QueryProfile q = c == null ? QueryProfile.getDefaultInstance() : c.getObj();
    
    try {
      return work.getContext().getConfig().getMapper().writeValueAsString(q);
    } catch (JsonProcessingException e) {
      // TODO Auto-generated catch block
      System.out.println(e);
      e.printStackTrace();
    }
    
    return "";
  }

  @GET
  @Path("v1/plan/{queryid}")
  @Produces(MediaType.APPLICATION_JSON)
  public String getPlan(@PathParam("queryid") String queryId) {
    if (planmap.containsKey(queryId)) {
        try {
          return work.getContext().getConfig().getMapper().writeValueAsString(planmap.get(queryId));
        } catch (JsonProcessingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
    }
    return "";
  }
}
