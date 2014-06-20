<#-- Licensed to the Apache Software Foundation (ASF) under one or more contributor
  license agreements. See the NOTICE file distributed with this work for additional
  information regarding copyright ownership. The ASF licenses this file to
  You under the Apache License, Version 2.0 (the "License"); you may not use
  this file except in compliance with the License. You may obtain a copy of
  the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required
  by applicable law or agreed to in writing, software distributed under the
  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
  OF ANY KIND, either express or implied. See the License for the specific
  language governing permissions and limitations under the License. -->

<#include "*/generic.ftl">
<#macro page_head>
<link href="/www/style.css" rel="stylesheet">

<script src="http://d3js.org/d3.v3.min.js"></script>
<script src="http://cpettitt.github.io/project/dagre-d3/latest/dagre-d3.js"></script>
<script src="/www/graph.js"></script>
</#macro>

<#macro page_body>
  <a href="/queries">back</a><br/>
  <div class="page-header">
  </div>
  <h2>Query</h2>
  <form role="form" action="/query" method="POST">
    <div class="form-group">
      <textarea class="form-control" id="query" name="query">${model.getProfile().query}</textarea>
    </div>
    <div class="form-group">
      <div class="radio-inline">
        <label>
          <input type="radio" name="queryType" id="sql" value="SQL" checked>
          SQL
        </label>
      </div>
      <div class="radio-inline">
        <label>
          <input type="radio" name="queryType" id="physical" value="PHYSICAL">
          PHYSICAL
        </label>
      </div>
      <div class="radio-inline">
        <label>
          <input type="radio" name="queryType" id="logical" value="LOGICAL">
          LOGICAL
        </label>
      </div>
    </div>
    <button type="submit" class="btn btn-default">Re-run query</button>
  </form>
  <div class="page-header">
    <h2>Visualized Plan</h2>
    <button id="renderbutton" class="btn btn-default">generate</button>
    <svg id="svg-canvas">
      <g transform="translate(20, 20)"/>
    </svg>
  </div>
  <div class="page-header">
    <h2>Physical Plan</h2>
  </div>
  <p><pre>${model.profile.plan}</pre></p>
  <div class="page-header">
  <h2>Profile Summary</h2>
  </div>
  <p>${model.toString()}</p>
  <div class="page-header">
  <h2>Complete Profile</h2>
  </div>
  <p><pre>${model.profile.toString()}</pre></p>
</#macro>

<@page_html/>
