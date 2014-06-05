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
<a href="/queries">back</a><br />

<pre>
${model.query}
</pre> 
<br /><br />
<pre>
${model.plan}
</pre>

<pre>
${model.id}
</pre>

<pre>
<#list model.getFragmentProfileList() as major>
	${major.getMajorFragmentId()}
	<#list major.getMinorFragmentProfileList() as minor>
		${minor.getState()}
		${minor.getStartTime()}
		${minor.getEndTime()}
		${minor.getMinorFragmentId()}
		<#list minor.getOperatorProfileList() as op>
			${op.getSetupNanos()}
			${op.getProcessNanos()}
			${op.getOperatorId()}
			<#list op.getInputProfileList() as input>
				${input.getRecords()}
				${input.getBatches()}
			</#list>
		</#list>
	</#list>
</#list>
</pre>


<pre>
${model.toString()}
</pre>



<html>
