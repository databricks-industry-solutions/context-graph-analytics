# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Context Graphs for Investigation Workflows
# MAGIC 
# MAGIC ## Use cases
# MAGIC 
# MAGIC * An enterprise might have multiple IAM/SSO, AD domains, authn systems including local unix authn. An analyst investigating an incident will need to manually resolve these identities. Context graphs provide `same_as` edges that capture the results from any entity resolution package and these edges can be traversed like any other relationships.
# MAGIC * Impact analysis (blast radius)
# MAGIC   * Given a compromised user account, what apps, ip addresses etc. has this account touched across the enterprise?
# MAGIC   * Given a compromised app or ip address, who are the users being affected?
# MAGIC * Investigate lateral movement as the attacker steals credentials, escalate privileges and move from one system to another (https://www.crowdstrike.com/cybersecurity-101/lateral-movement/) 
# MAGIC * Investigate insider threat by exploring relationships with HR data sources

# COMMAND ----------

dbutils.widgets.removeAll()
#dbutils.widgets.text("travel_node", "", "travel along node (id or name): ")
#travel_node = dbutils.widgets.get("travel_node")

# COMMAND ----------

# MAGIC %pip install bokeh

# COMMAND ----------

import networkx as nx
from bokeh.io import output_notebook, show, save
from bokeh.models import Range1d, Circle, ColumnDataSource, MultiLine, HoverTool
from bokeh.plotting import figure
from bokeh.plotting import from_networkx
from bokeh.embed import file_html
from bokeh.resources import CDN

def get_node_color(node_type):
  if node_type[:4] == "user":
    return "dimgray"  
  if node_type == "suricata-alert":
    return "red"
  if node_type == "ipAddress":
    return "#F39C12"
  if node_type == "hash":
    return "#F9E79F"
  if node_type == "fqdn":
    return "#F1C40F"
  if node_type == "app":
    return "dodgerblue"
  return "lightblue"

def stringify(col):
  if col is None:
    return ""
  return str(col)

def add_edges_from_df(G, edges_df):
  n=0
  for (src, time_bkt, sub_type, sub_id, sub_name, pred, pred_status, obj_type, obj_id, obj_name, first_seen, last_seen, cnt) in edges_df.collect():
    nodes = [(sub_id, {"type": sub_type, "name": sub_name, "color": get_node_color(sub_type)}),
             (obj_id, {"type": obj_type, "name": obj_name, "color": get_node_color(obj_type)})]
    G.add_nodes_from(nodes)
    edges = [(sub_id, obj_id, {"name": pred, "status": stringify(pred_status), "cnt": stringify(cnt), "firstseen": stringify(first_seen), "lastseen": stringify(last_seen)})]
    G.add_edges_from(edges)
    n+=1
  return n

# Renders the graph using Bokeh library
def displayGraph(G, title):

  #Create a plot — set dimensions, toolbar, and title
  plot = figure(tools="pan,wheel_zoom,save,reset", active_scroll='wheel_zoom',
            x_range=Range1d(-20.1, 20.1), y_range=Range1d(-20.1, 20.1), title=title)

  #Create a network graph object with spring layout
  # https://networkx.github.io/documentation/networkx-1.9/reference/generated/networkx.drawing.layout.spring_layout.html
  network_graph = from_networkx(G, nx.spring_layout, scale=10, center=(0, 0))

  #Set node size and color
  network_graph.node_renderer.glyph = Circle(size=20, fill_color='color')

  #Set edge opacity and width
  network_graph.edge_renderer.glyph = MultiLine(line_alpha=0.5, line_width=1)
  
  hover_nodes = HoverTool(
    tooltips=[("id", "@index"), ("name", "@name"), ("type", "@type")],
    renderers=[network_graph.node_renderer]
  )
  hover_edges = HoverTool(
    tooltips=[("name","@name"),("status","@status"), ("cnt", "@cnt"), ("firstseen", "@firstseen"), ("lastseen", "@lastseen")],
    renderers=[network_graph.edge_renderer], line_policy="interp"
  )
  
  #Add network graph to the plot
  plot.renderers.append(network_graph)
  plot.add_tools(hover_edges, hover_nodes)
  html = file_html(plot, CDN)
  displayHTML(html)

# Generate the sql query for the graph search/navigation using a template
# Incorporates the same_as edges in the search
def generate_sql_query(node_filter, time_frame, same_as=True):
  if same_as:
    return f"""
WITH sub_matches AS (
  select
    *
  from
    solacc_cga.v_edges_day
  where
    time_bkt = '{time_frame}' 
    AND ( sub_id = '{node_filter}'
    or sub_name = '{node_filter}')
),
sub_same_as AS (
  SELECT
    s1.src,
    s1.time_bkt,
    s1.sub_type,
    s1.sub_id,
    s1.sub_name,
    s1.pred,
    s1.pred_status,
    s1.obj_type,
    s1.obj_id,
    s1.obj_name,
    NULL as first_seen,
    NULL as last_seen,
    NULL as cnt
  FROM
    sub_matches AS e1
    JOIN solacc_cga.same_as AS s1 ON e1.sub_id = s1.sub_id
),
obj_matches AS (
  select
    *
  from
    solacc_cga.v_edges_day
  where
    time_bkt = '{time_frame}' 
    AND ( obj_id = '{node_filter}'
    or obj_name = '{node_filter}' )
),
obj_same_as AS (
  SELECT
    s3.src,
    s3.time_bkt,
    s3.sub_type,
    s3.sub_id,
    s3.sub_name,
    s3.pred,
    s3.pred_status,
    s3.obj_type,
    s3.obj_id,
    s3.obj_name,
    NULL as first_seen,
    NULL as last_seen,
    NULL as cnt
  FROM
    obj_matches AS e3
    JOIN solacc_cga.same_as AS s3 ON e3.obj_id = s3.sub_id
)
SELECT
  *
FROM
  sub_matches
UNION
SELECT
  e2.*
FROM
  sub_same_as AS s2
  JOIN solacc_cga.v_edges_day AS e2 ON s2.obj_id = e2.sub_id AND e2.time_bkt = '{time_frame}' 
UNION
SELECT
  *
FROM
  obj_matches
UNION
SELECT
  e4.*
FROM
  obj_same_as AS s4
  JOIN solacc_cga.v_edges_day AS e4 ON s4.obj_id = e4.obj_id AND e4.time_bkt = '{time_frame}' 
UNION
SELECT
  *
FROM
  sub_same_as
UNION
SELECT
  *
FROM
  obj_same_as
"""
  return """
select
    *
from solacc_cga.v_edges
where time_bkt = '{time_frame}' 
  AND ( sub_id = '{node_filter}' 
    OR sub_name = '{node_filter}'
    OR obj_id = '{node_filter}'
    OR obj_name = '{node_filter}'
    OR sub_name ilike '%{node_filter}%'
    OR obj_name ilike '%{node_filter}%'
    )  
  """

# COMMAND ----------

# DBTITLE 1,Reset/Initializes the graph visualization
import networkx as nx

G = nx.Graph()
g_df = None


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Demo Setup
# MAGIC 
# MAGIC You are an incident responder or an analyst investigating an advanced persistent threat. The following command simulates an investigation process that builds an investigation graph iteratively through a series of edge queries. You will build the investigation graph by changing the search parameter and executing the following command. To reset the investigation graph, you will need to execute the command prior to this cell.
# MAGIC 
# MAGIC The output of the following command consists of three components:
# MAGIC 
# MAGIC 1. The table of edges from the most recent search query
# MAGIC 2. The graph visualization
# MAGIC 3. The cumulative table of edges from the search history since the last reset.
# MAGIC 
# MAGIC ## Demo Story
# MAGIC 
# MAGIC * Suppose `maria.cole@chang-fisher.com` has been compromised. Let's pull back all the edges incident on that person.
# MAGIC     * You can hover over the edges or nodes in the graph visualization to see details.
# MAGIC     * It turns out that maria has an old account from a company Summers.info that the Chang-Fisher company has acquired.
# MAGIC     * Moreover, maria got married and changed her lastname.
# MAGIC     * Note all the IP addresses, apps, and resources that Maria has signed in from/to. 
# MAGIC     * This is an example of a `same_as` edge produced by an entity resolution package.
# MAGIC * Suppose we know from EDR alerts that `megan.chang@chang-fisher.com` is also compromised. Let's pull back her edges.
# MAGIC     * Observe that there are common IP addresses, apps and resources shared by the two compromised accounts. Any of these could be the pivot point the actor used to harvest these accounts and credentials.
# MAGIC * It is likely now that all the IP addresses Megan is associated with has been compromised. Let's expand on the IP address `165.225.221.4`.
# MAGIC     * Observe that Kyle Schultz also signs in from that IP address, so his account might be compromised too.
# MAGIC     * Let's pull back all edges that Kyle is associated with.
# MAGIC * Now let your imagination flow a bit and imagine how we can use graph algorithms like Breadth First Search and Strongly connected components to perform automated impact analysis.

# COMMAND ----------

# DBTITLE 1,Navigate and build the investigation graph as part of the investigation process
# this query needs to be highly selective, so that it only brings back a small subset of edges to render
# it is foolish to try and render a 1M node graph
# note that dbutils.widget parametrization is an option that was tried, but the following is easier and more stable

time_frame = "2022-07-20T00:00:00.000+0000"
travel_node = "maria.cole@chang-fisher.com"
#travel_node = "megan.chang@chang-fisher.com"
#travel_node = "165.225.221.4"
#travel_node = "199.106.8.190"
#travel_node = "kyle.schultz@chang-fisher.com"
use_same_as = True

sql_str = generate_sql_query( travel_node, time_frame, use_same_as )

edges_df = spark.sql(sql_str)

if g_df is None:
  g_df = edges_df
else:
  g_df = g_df.union(edges_df)

# display edges from most recent query
display(edges_df)

add_edges_from_df(G, edges_df)
# display the investigation graph so far
displayGraph(G, "Identity Context")

# display the edges of the entire search history since last reset
display(g_df)

# COMMAND ----------

