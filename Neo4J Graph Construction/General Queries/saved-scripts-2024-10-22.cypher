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