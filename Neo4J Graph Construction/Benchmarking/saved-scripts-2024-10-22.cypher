// Delete Medium and Large Node

MATCH (n:BusStop)
WHERE n.medium = true
DETACH DELETE n;

MATCH (n:BusStop)
WHERE n.large = true
DETACH DELETE n;

//Step 1 - Assign Attributes for the Small Graph

MATCH (n)
SET n.small = true;

MATCH ()-[r]->()
SET r.small = true;


//Step 2 -Create 10000 Nodes for the Medium Graph:

WITH range(1, 10000) AS newNodes
UNWIND newNodes AS id
CREATE (n:BusStop {name: 'Medium Bus Stop ' + id, medium: true});


//Step 3 - Connect Each Node to the Next One

MATCH (a:BusStop {medium: true})
WITH a ORDER BY a.name
WITH collect(a) AS stops
UNWIND range(0, size(stops) - 2) AS idx
WITH stops[idx] AS stopA, stops[idx + 1] AS stopB
CREATE (stopA)-[r:CONNECTS_TO {traffic_flow: round(rand() * 100), medium: true}]->(stopB);


// Step 4 - Create 30000 New BusStop Nodes

WITH range(1, 30000) AS newNodes
UNWIND newNodes AS id
CREATE (n:BusStop {name: 'Large Bus Stop ' + id, large: true});


// Step 5 - Connect Each Node to the Next One

MATCH (a:BusStop {large: true})
WITH a ORDER BY a.name
WITH collect(a) AS stops
UNWIND range(0, size(stops) - 2) AS idx
WITH stops[idx] AS stopA, stops[idx + 1] AS stopB
CREATE (stopA)-[r:CONNECTS_TO {traffic_flow: round(rand() * 100), large: true}]->(stopB);