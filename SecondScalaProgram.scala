// Databricks notebook source
<a href="$./anothernotebook">Detailed Customer Review Reports</a>



displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill="yellow" />
   Sorry, your browser does not support inline SVG.
</svg>""")


val colorsRDD = sc.parallelize(Array((197,27,125), (222,119,174), (241,182,218), (253,244,239), (247,247,247), (230,245,208), (184,225,134), (127,188,65), (77,146,33)))
val colors = colorsRDD.collect()


displayHTML(s"""
<!DOCTYPE html>
<meta charset="utf-8">
<style>

path {
  fill: yellow;
  stroke: #000;
}

circle {
  fill: #fff;
  stroke: #000;
  pointer-events: none;
}

.PiYG .q0-9{fill:rgb${colors(0)}}
.PiYG .q1-9{fill:rgb${colors(1)}}
.PiYG .q2-9{fill:rgb${colors(2)}}
.PiYG .q3-9{fill:rgb${colors(3)}}
.PiYG .q4-9{fill:rgb${colors(4)}}
.PiYG .q5-9{fill:rgb${colors(5)}}
.PiYG .q6-9{fill:rgb${colors(6)}}
.PiYG .q7-9{fill:rgb${colors(7)}}
.PiYG .q8-9{fill:rgb${colors(8)}}

</style>
<body>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.6/d3.min.js"></script>
<script>

var width = 960,
    height = 500;

var vertices = d3.range(100).map(function(d) {
  return [Math.random() * width, Math.random() * height];
});

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("class", "PiYG")
    .on("mousemove", function() { vertices[0] = d3.mouse(this); redraw(); });

var path = svg.append("g").selectAll("path");

svg.selectAll("circle")
    .data(vertices.slice(1))
  .enter().append("circle")
    .attr("transform", function(d) { return "translate(" + d + ")"; })
    .attr("r", 2);

redraw();

function redraw() {
  path = path.data(d3.geom.delaunay(vertices).map(function(d) { return "M" + d.join("L") + "Z"; }), String);
  path.exit().remove();
  path.enter().append("path").attr("class", function(d, i) { return "q" + (i % 9) + "-9"; }).attr("d", String);
}

</script>
  """)

// COMMAND ----------



// COMMAND ----------

