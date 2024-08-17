import pandas as pd

# Load the CSV files
stop_times = pd.read_csv('stop_times.csv')
trips = pd.read_csv('trips.csv')
routes = pd.read_csv('routes.csv')
stops = pd.read_csv('stops.csv') 

# Merge stop_times with trips to get the route_id for each trip
stop_times_trips = pd.merge(stop_times, trips, on='trip_id', how='left')

# Now merge with routes to get the route details
stop_times_routes = pd.merge(stop_times_trips, routes, on='route_id', how='left')

# Merge with stops to get the stop_name
stop_times_routes_stops = pd.merge(stop_times_routes, stops, on='stop_id', how='left')

# Select only the necessary columns including stop_sequence and stop_name
route_stop_df = stop_times_routes_stops[['route_id', 'stop_id', 'stop_name', 'stop_sequence']]

# Remove duplicates if any (just in case)
route_stop_df = route_stop_df.drop_duplicates()

# Save the result to a new CSV file
route_stop_df.to_csv('route_stop_mapping.csv', index=False)
