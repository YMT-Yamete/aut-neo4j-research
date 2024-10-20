import psutil
import time
from neo4j import GraphDatabase

# Define your queries as Python variables

# 1. Query for All Shortest Path with Average Traffic Flow
query1_shortest_path_avg_traffic = """
MATCH (start:BusStop {name: 'Holy Trinity Cathedral'}), (end:BusStop {name: 'Parnell Shops'})
MATCH path = shortestPath((start)-[:CONNECTS_TO*]->(end))
WITH path, nodes(path) AS stops, relationships(path) AS rels
UNWIND stops AS stop
MATCH (stop)-[:PART_OF]->(route:Route)
WITH stop, route.route_id AS commonRouteID, stops, rels
WHERE ALL(s IN stops WHERE (s)-[:PART_OF]->(:Route {route_id: commonRouteID}))
WITH collect(distinct stop.name) AS BusStop, commonRouteID AS RouteID, rels
RETURN BusStop, 
       RouteID, 
       ROUND(REDUCE(totalFlow = 0, r IN rels | totalFlow + r.traffic_flow) / SIZE(rels), 2) AS AvgTrafficFlow
"""

# 2. Query for Congestion Alerts Based on Traffic Flow
query2_congestion_alerts = """
MATCH (start:BusStop)-[r:CONNECTS_TO]->(end:BusStop)
MATCH (start)-[:PART_OF]->(route:Route)
WHERE r.traffic_flow > 90
RETURN route.route_id AS Route, start.name AS From, end.name AS To, r.traffic_flow AS TrafficFlow
ORDER BY r.traffic_flow DESC
"""

# 3. Query for Identifying Bus Stops with the Most Waiting People
query3_busiest_stops = """
MATCH (stop:BusStop)
RETURN stop.name AS BusStop, 
       stop.waiting_people AS WaitingPeople
ORDER BY WaitingPeople DESC
LIMIT 100
"""

# 4. Query for Visualizing the Full Bus Route with All Conditions
query4_full_route_visualization = """
MATCH (start:BusStop)-[r:CONNECTS_TO]->(end:BusStop)-[:PART_OF]->(route:Route)
WHERE route.route_id = 'INN-202' AND r.traffic_flow IS NOT NULL AND r.weather IS NOT NULL AND r.incidents IS NOT NULL
RETURN start.name AS From, 
       end.name AS To, 
       route.route_id AS RouteID, 
       r.traffic_flow AS TrafficFlow, 
       r.weather AS Weather, 
       r.incidents AS Incidents
ORDER BY From, To
"""

# Function to execute a query and measure the time and memory usage
def run_query_and_monitor_memory(driver, query):
    process = None
    for proc in psutil.process_iter(['pid', 'name']):
        if 'neo4j' in proc.info['name'].lower() or 'java' in proc.info['name'].lower():
            process = proc
            break
    
    if not process:
        raise Exception("Neo4j process not found. Make sure Neo4j is running.")

    # Measure the execution time and memory usage
    start_time = time.time()
    initial_memory = process.memory_info().rss / (1024 * 1024)  # Convert from bytes to MB
    with driver.session() as session:
        result = session.run(query)
        records = list(result)
    end_time = time.time()
    execution_time = end_time - start_time
    final_memory = process.memory_info().rss / (1024 * 1024)  # Memory in MB

    # Calculate memory usage during the query execution
    memory_usage = final_memory - initial_memory
    return records, execution_time, memory_usage

# Setup the Neo4j driver
uri = "bolt://localhost:7687"
user = "neo4j"
password = "aucklandtransportpassword"  
driver = GraphDatabase.driver(uri, auth=(user, password))

# Choose the query to run (change this to run a different query)
chosen_query = query4_full_route_visualization

# Variables to store total execution time and memory usage
total_execution_time = 0
total_memory_usage = 0
last_query_records = []

# Run the query 10 times and compute averages
for i in range(10):
    print(f"Running iteration {i+1}...")
    records, execution_time, memory_usage = run_query_and_monitor_memory(driver, chosen_query)
    total_execution_time += execution_time
    total_memory_usage += memory_usage
    last_query_records = records  # Store the last result for display
    print(f"Iteration {i+1}: Execution Time = {execution_time:.2f}s, Memory Usage = {memory_usage:.2f} MB")

# Calculate overall averages
average_execution_time = total_execution_time / 10
average_memory_usage = total_memory_usage / 10

print(f"\nAverage Execution Time: {average_execution_time:.2f}s")
print(f"Average Memory Usage: {average_memory_usage:.2f} MB")

# Print the results of the last query run
# print("\nLast Query Result:")
# for record in last_query_records:
#     print(record)

# Close the driver connection
driver.close()
