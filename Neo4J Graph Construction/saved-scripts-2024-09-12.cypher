//Delete All Data
MATCH (n)
DETACH DELETE n;


// F - All Routes Passing Through a Stop
MATCH (stop:BusStop {name: 'Holy Trinity Cathedral'})-[:PART_OF]->(route:Route)
RETURN route.route_id AS RouteID


//F - All Shortest Path but without Interchange
MATCH (start:BusStop {name: 'Holy Trinity Cathedral'}), (end:BusStop {name: 'Parnell Shops'})
MATCH path = shortestPath((start)-[:CONNECTS_TO*]->(end))
WITH path, nodes(path) AS stops
UNWIND stops AS stop
MATCH (stop)-[:PART_OF]->(route:Route)
WITH stop, route.route_id AS commonRouteID, stops
// Ensure all stops in the path belong to the same route
WHERE ALL(s IN stops WHERE (s)-[:PART_OF]->(:Route {route_id: commonRouteID}))
RETURN collect(distinct stop.name) AS BusStop, commonRouteID AS RouteID


// F - Betweeness Centrality 
// F -  how often a bus stop appears on the shortest paths between other stops
CALL gds.graph.project(
    'busStopGraph',
    'BusStop',
    'CONNECTS_TO'
)
YIELD graphName
WITH graphName
CALL gds.betweenness.stream(graphName)
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).name AS BusStop, score AS centrality
ORDER BY centrality DESC
LIMIT 5


// F - Degree Centrality - Top 5 Most Connected Bus Stops
MATCH (stop:BusStop)-[r:CONNECTS_TO]->()
WITH stop, count(r) AS degree
ORDER BY degree DESC
LIMIT 5
RETURN stop.name AS BusStop, degree


// F - Path Traversal
// Traverse the bus stops starting from a particular stop and see where you can reach within a certain number of hops.
MATCH (start:BusStop {name: 'Holy Trinity Cathedral'})
CALL apoc.path.spanningTree(start, {
    relationshipFilter: 'CONNECTS_TO>',
    maxLevel: 5
})
YIELD path
RETURN nodes(path) AS stops


//F - Shortest Path Graph without Bus Route Name
MATCH (start:BusStop {name: 'Holy Trinity Cathedral'}), (end:BusStop {name: 'AUT City Campus'})
MATCH path = shortestPath((start)-[:CONNECTS_TO*]->(end))
RETURN path
LIMIT 1


// F - Visualize bus route
MATCH (stop:BusStop)-[r:CONNECTS_TO]->(nextStop:BusStop)
WHERE (stop)-[:PART_OF]->(:Route {route_id: 'TMK-202'}) 
  AND (nextStop)-[:PART_OF]->(:Route {route_id: 'TMK-202'})
RETURN stop, nextStop, r;


//F- Print out Bus Stop Nodes with stop_id 
MATCH (stop:BusStop {stop_id: '7193-7ada3d13'})
RETURN stop;


//Step 1: Create Route Nodes for Inbound
LOAD CSV WITH HEADERS FROM 'file:///auckland_transport_data_inbound.csv' AS row
MERGE (route:Route {route_id: row.route_id});

// Create Route Nodes for Outbound
LOAD CSV WITH HEADERS FROM 'file:///auckland_transport_data_outbound.csv' AS row
MERGE (route:Route {route_id: row.route_id});


//Step 2: Create Bus Stops and Connect Them to Routes (Inbound)
LOAD CSV WITH HEADERS FROM 'file:///auckland_transport_data_inbound.csv' AS row
MERGE (stop:BusStop {stop_id: row.stop_id, route_id: row.route_id, direction: 'inbound'})
ON CREATE SET stop.name = row.stop_name,
              stop.lat = toFloat(row.stop_lat),
              stop.lon = toFloat(row.stop_lon),
              stop.stop_sequence = toInteger(row.stop_sequence)
WITH stop, row
MATCH (route:Route {route_id: row.route_id})
MERGE (stop)-[:PART_OF {sequence: toInteger(row.stop_sequence)}]->(route);

// Create Bus Stops and Connect Them to Routes (Outbound)
LOAD CSV WITH HEADERS FROM 'file:///auckland_transport_data_outbound.csv' AS row
MERGE (stop:BusStop {stop_id: row.stop_id, route_id: row.route_id, direction: 'outbound'})
ON CREATE SET stop.name = row.stop_name,
              stop.lat = toFloat(row.stop_lat),
              stop.lon = toFloat(row.stop_lon),
              stop.stop_sequence = toInteger(row.stop_sequence)
WITH stop, row
MATCH (route:Route {route_id: row.route_id})
MERGE (stop)-[:PART_OF {sequence: toInteger(row.stop_sequence)}]->(route);


//Step 3: Connect Bus Stops in Sequence Within Inbound Routes
LOAD CSV WITH HEADERS FROM 'file:///auckland_transport_data_inbound.csv' AS row
WITH row
MATCH (currentStop:BusStop {stop_id: row.stop_id, route_id: row.route_id, direction: 'inbound', stop_sequence: toInteger(row.stop_sequence)})
MATCH (nextStop:BusStop {route_id: row.route_id, direction: 'inbound', stop_sequence: toInteger(row.stop_sequence) + 1})
MERGE (currentStop)-[r:CONNECTS_TO]->(nextStop)
ON CREATE SET r.distance = toFloat(row.distance_to_next_stop),
              r.travel_time = toFloat(row.travel_time_to_next_stop);


//Step 4: Connect Bus Stops in Sequence Within Outbound Routes
LOAD CSV WITH HEADERS FROM 'file:///auckland_transport_data_outbound.csv' AS row
WITH row
MATCH (currentStop:BusStop {stop_id: row.stop_id, route_id: row.route_id, direction: 'outbound', stop_sequence: toInteger(row.stop_sequence)})
MATCH (nextStop:BusStop {route_id: row.route_id, direction: 'outbound', stop_sequence: toInteger(row.stop_sequence) + 1})
MERGE (currentStop)-[r:CONNECTS_TO]->(nextStop)
ON CREATE SET r.distance = toFloat(row.distance_to_next_stop),
              r.travel_time = toFloat(row.travel_time_to_next_stop);


// Step 5: Delete stop_sequence, route_id, and direction attributes from all BusStop nodes
MATCH (stop:BusStop)
REMOVE stop.stop_sequence, stop.route_id, stop.direction;


// Step 6: Transfer Outgoing Relationships and Attributes (for both CONNECTS_TO and PART_OF)

// Transfer CONNECTS_TO relationships
MATCH (a:BusStop)-[r:CONNECTS_TO]->(b:BusStop)
WITH a, r, b
MATCH (c:BusStop {stop_id: a.stop_id})
WHERE ID(a) <> ID(c)
MERGE (c)-[newRel:CONNECTS_TO]->(b)
ON CREATE SET newRel = r;

// Transfer PART_OF relationships
MATCH (a:BusStop)-[r:PART_OF]->(route:Route)
WITH a, r, route
MATCH (c:BusStop {stop_id: a.stop_id})
WHERE ID(a) <> ID(c)
MERGE (c)-[newRel:PART_OF]->(route)
ON CREATE SET newRel = r;


// Step 7: Transfer Incoming Relationships and Attributes (for both CONNECTS_TO and PART_OF)

// Transfer CONNECTS_TO relationships
MATCH (a:BusStop)<-[r:CONNECTS_TO]-(b:BusStop)
WITH a, r, b
MATCH (c:BusStop {stop_id: a.stop_id})
WHERE ID(a) <> ID(c)
MERGE (b)-[newRel:CONNECTS_TO]->(c)
ON CREATE SET newRel = r;

// Transfer PART_OF relationships (if any incoming PART_OF relationships exist)
MATCH (a:BusStop)<-[r:PART_OF]-(route:Route)
WITH a, r, route
MATCH (c:BusStop {stop_id: a.stop_id})
WHERE ID(a) <> ID(c)
MERGE (route)-[newRel:PART_OF]->(c)
ON CREATE SET newRel = r;


// Step 8: Delete Duplicate Nodes

MATCH (n:BusStop)
WITH n.stop_id AS sid, COLLECT(n) AS nodes
WHERE SIZE(nodes) > 1
FOREACH (n IN TAIL(nodes) | DETACH DELETE n);