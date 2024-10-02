import random
import time
from neo4j import GraphDatabase

# Connect to Neo4j
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "aucklandtransportpassword"))
session = driver.session()

# Step 1: Extract all bus stops from Neo4j
def get_all_bus_stops(session):
    query = """
    MATCH (stop:BusStop)
    RETURN stop.name AS stop_name
    """
    result = session.run(query)
    stops = [record['stop_name'] for record in result]
    return stops

# Step 2: Simulate traffic, weather, and incidents between bus stops
def simulate_connection_data(session, stop1, stop2):
    traffic_flow = random.randint(0, 100)

    # Weather condition probabilities: Clear 90%, Rain 5%, Fog 5%
    weather = random.choices(['Clear', 'Rain', 'Fog'], weights=[90, 5, 5])[0]
    
    # Incident likelihood: 0 (No Incident) 999 times out of 1000, 1 (Incident) 1 time out of 1000
    incidents = random.choices([0, 1], weights=[999, 1])[0]

    query = """
    MATCH (stop1:BusStop {name: $stop1})-[r:CONNECTS_TO]->(stop2:BusStop {name: $stop2})
    SET r.traffic_flow = $traffic_flow, r.weather = $weather, r.incidents = $incidents
    """
    session.run(query, stop1=stop1, stop2=stop2, traffic_flow=traffic_flow, weather=weather, incidents=incidents)

    # Log the simulation status for the connection
    print(f"Updated connection between {stop1} and {stop2}: Traffic Flow={traffic_flow}, Weather={weather}, Incidents={incidents}")

# Step 3: Simulate waiting people at each bus stop
def simulate_waiting_people(session, stop):
    # Number of waiting people, max limit set to 15
    waiting_people = random.randint(0, 15)

    query = """
    MATCH (stop:BusStop {name: $stop})
    SET stop.waiting_people = $waiting_people
    """
    session.run(query, stop=stop, waiting_people=waiting_people)

    # Log the simulation status for the stop
    print(f"Updated stop {stop}: Waiting People={waiting_people}")

# Step 4: Main simulation loop (runs indefinitely)
def run_simulation():
    bus_stops = get_all_bus_stops(session)

    iteration = 0
    while True:  # Infinite loop to keep running the simulation
        print(f"--- Simulation iteration {iteration} ---")
        for i in range(len(bus_stops) - 1):
            simulate_connection_data(session, bus_stops[i], bus_stops[i + 1])  # Simulate between consecutive stops
            simulate_waiting_people(session, bus_stops[i])  # Simulate people waiting at each stop

        simulate_waiting_people(session, bus_stops[-1])  # Simulate for the last stop

        print(f"--- End of iteration {iteration}, sleeping for 60 seconds ---\n")
        iteration += 1
        time.sleep(60)  # Updates data every 60 seconds

# Run the simulation
try:
    run_simulation()
except KeyboardInterrupt:
    print("Simulation stopped by user.")
    session.close()
