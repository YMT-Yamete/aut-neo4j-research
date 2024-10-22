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