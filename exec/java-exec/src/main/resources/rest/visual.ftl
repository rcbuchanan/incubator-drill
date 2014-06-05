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

<html>
<head>
	<script src="http://d3js.org/d3.v3.min.js" charset="utf-8"></script>
	<script src="http://cpettitt.github.io/project/dagre-d3/latest/dagre-d3.js" ></script>
	<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js" ></script>
	<script src="http://localhost:8000/visual.js"></script>
	<style>
		g.type-TK > rect {
		fill: #00ffd0;
		}

		svg {
			border: 1px solid #FFF;
			overflow: hidden;
		}

		.node rect {
			stroke: #999;
			stroke-width: 1px;
			fill: #fff;
		}

		.edgeLabel rect {
			fill: #fff;
		}

		.edgePath path {
			stroke: #333;
			stroke-width: 1.5px;
			fill: none;
		}
	</style>
</head>
<body onload="onload('${model}')">
	<div id="attach">
		<svg id="svg-canvas" width=800 height=600>
			<g transform="translate(20, 20)"/>
		</svg>
	</div>
</body>
</html>