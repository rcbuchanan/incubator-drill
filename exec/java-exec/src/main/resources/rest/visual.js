function onload(queryid) {
	$.ajax({
		type: "GET",
		dataType: "json",
		url: "http://localhost:8047/v1/plan/" + queryid,
		success: function (plan) {
			var pg = plan.graph;
			var g = new dagreD3.Digraph();

			for (var i = 0; i < pg.length; i++) {
				g.addNode(pg[i]["@id"], {
					label: pg[i].pop,
					nodeclass: "type-blah"
				});
			}
  			
			for (var i = 0; i < pg.length; i++) {
				if (pg[i].child)
					g.addEdge(null, pg[i]["@id"], pg[i].child);
				if (pg[i].left)
					g.addEdge(null, pg[i]["@id"], pg[i].left);
				if (pg[i].right)
						g.addEdge(null, pg[i]["@id"], pg[i].right);
			}
			
			var renderer = new dagreD3.Renderer();
			var oldDrawNodes = renderer.drawNodes();
			renderer.drawNodes(function(graph, root) {
			var svgNodes = oldDrawNodes(graph, root);
			svgNodes.each(function(u) {
				d3.select(this).classed(graph.node(u).nodeclass, true); });
				return svgNodes;
			});
			var layout = renderer.run(g, d3.select("svg g"));
			d3.select("svg")
				.attr("width", layout.graph().width + 40)
				.attr("height", layout.graph().height + 40);
		},
		error: function (x, y, z) {
			console.log(x);
			console.log(y);
			console.log(z);
		}
	});
	
	$.ajax({
		type: "GET",
			dataType: "json",
			url: "http://localhost:8047/v1/fpl/" + queryid,
			success: function (data) {
				gv = data;
			},
			error: function (x, y, z) {
				console.log(x);
				console.log(y);
				console.log(z);
			}
		});
}