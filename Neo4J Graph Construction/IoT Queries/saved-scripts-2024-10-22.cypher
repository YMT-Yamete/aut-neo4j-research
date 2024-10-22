//iot-F - All Shortest Path, with Average Traffic Flow
MATCH (start:BusStop {name: 'Holy Trinity Cathedral'}), (end:BusStop {name: 'Parnell Shops'})
MATCH path = shortestPath((start)-[:CONNECTS_TO*]->(end))
WITH path, nodes(path) AS stops, relationships(path) AS rels
UNWIND stops AS stop
MATCH (stop)-[:PART_OF]->(route:Route)
WITH stop, route.route_id AS commonRouteID, stops, rels
// Ensure all stops in the path belong to the same route
WHERE ALL(s IN stops WHERE (s)-[:PART_OF]->(:Route {route_id: commonRouteID}))
WITH collect(distinct stop.name) AS BusStop, commonRouteID AS RouteID, rels
// Calculate the average traffic flow for the path
RETURN BusStop, 
       RouteID, 
       ROUND(REDUCE(totalFlow = 0, r IN rels | totalFlow + r.traffic_flow) / SIZE(rels), 2) AS AvgTrafficFlow


//iot-F - Congestion Alerts Based on Traffic Flow
MATCH (start:BusStop)-[r:CONNECTS_TO]->(end:BusStop)
MATCH (start)-[:PART_OF]->(route:Route)
WHERE r.traffic_flow > 90
RETURN route.route_id AS Route, start.name AS From, end.name AS To, r.traffic_flow AS TrafficFlow
ORDER BY r.traffic_flow DESC


//iot-F - Identifying Bus Stops with the Most Waiting People
MATCH (stop:BusStop)
RETURN stop.name AS BusStop, 
       stop.waiting_people AS WaitingPeople
ORDER BY WaitingPeople DESC
LIMIT 100

//iot-F - Visualize the full bus route with all conditions
MATCH (start:BusStop)-[r:CONNECTS_TO]->(end:BusStop)-[:PART_OF]->(route:Route)
WHERE route.route_id = 'INN-202' AND r.traffic_flow IS NOT NULL AND r.weather IS NOT NULL AND r.incidents IS NOT NULL
RETURN start.name AS From, 
       end.name AS To, 
       route.route_id AS RouteID, 
       r.traffic_flow AS TrafficFlow, 
       r.weather AS Weather, 
       r.incidents AS Incidents
ORDER BY From, To