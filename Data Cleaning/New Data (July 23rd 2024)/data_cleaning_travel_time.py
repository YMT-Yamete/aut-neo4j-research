import pandas as pd
import numpy as np

def haversine(lat1, lon1, lat2, lon2):
    # Haversine formula to calculate the distance between two points on the Earth
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371  # Radius of Earth in kilometers
    return c * r

# Load the CSV files
stop_times = pd.read_csv('stop_times.csv')
trips = pd.read_csv('trips.csv')
routes = pd.read_csv('routes.csv')
stops = pd.read_csv('stops.csv')

# Merge stop_times with trips to get the route_id for each trip
stop_times_trips = pd.merge(stop_times, trips, on='trip_id', how='left')

# Now merge with routes to get the route details
stop_times_routes = pd.merge(stop_times_trips, routes, on='route_id', how='left')

# Merge with stops to get the stop_name, latitude, and longitude
stop_times_routes_stops = pd.merge(stop_times_routes, stops, on='stop_id', how='left')

# Calculate distances using the haversine formula
stop_times_routes_stops['distance_to_next_stop'] = haversine(
    stop_times_routes_stops['stop_lat'],
    stop_times_routes_stops['stop_lon'],
    stop_times_routes_stops['stop_lat'].shift(-1),
    stop_times_routes_stops['stop_lon'].shift(-1)
)

# Ensure that the distance is only calculated within the same route
stop_times_routes_stops['distance_to_next_stop'] = np.where(
    stop_times_routes_stops['route_id'] == stop_times_routes_stops['route_id'].shift(-1),
    stop_times_routes_stops['distance_to_next_stop'],
    np.nan
)

# Select only the necessary columns including stop_sequence, stop_name, and distance
route_stop_df = stop_times_routes_stops[['route_id', 'stop_id', 'stop_name', 'stop_sequence', 'distance_to_next_stop']]

# Remove duplicates if any (just in case)
route_stop_df = route_stop_df.drop_duplicates()

# Calculate travel time directly within the DataFrame, aligning with existing data
route_stop_df['departure_time'] = pd.to_timedelta(stop_times_routes_stops['departure_time'], errors='coerce')
route_stop_df['arrival_time_next'] = pd.to_timedelta(stop_times_routes_stops['arrival_time'].shift(-1), errors='coerce')

# Calculate travel time between current stop departure and next stop arrival
route_stop_df['travel_time_to_next_stop'] = (
    route_stop_df['arrival_time_next'] - route_stop_df['departure_time']
).dt.total_seconds() / 60  # Convert to minutes

# Drop the intermediate columns used for calculation
route_stop_df = route_stop_df.drop(columns=['departure_time', 'arrival_time_next'])

# Ensure travel time is set to NaN for the last stop in each route
route_stop_df['travel_time_to_next_stop'] = np.where(
    route_stop_df['route_id'] == route_stop_df['route_id'].shift(-1),
    route_stop_df['travel_time_to_next_stop'],
    np.nan
)

# Save the final result to a new CSV file
route_stop_df.to_csv('route_stop_mapping_with_distance_and_travel_time.csv', index=False)

print("Route stop mapping with distance and travel time to the next stop has been saved successfully!")
