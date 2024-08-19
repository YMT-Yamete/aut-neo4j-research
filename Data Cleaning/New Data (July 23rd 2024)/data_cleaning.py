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

# Calculate travel time between stops using scheduled arrival and departure times
stop_times_routes_stops['arrival_time'] = pd.to_timedelta(stop_times_routes_stops['arrival_time'], errors='coerce')
stop_times_routes_stops['departure_time'] = pd.to_timedelta(stop_times_routes_stops['departure_time'], errors='coerce')

# Calculate travel time to the next stop
stop_times_routes_stops['travel_time_to_next_stop'] = (
    stop_times_routes_stops['arrival_time'].shift(-1) - stop_times_routes_stops['departure_time']
).dt.total_seconds() / 60  # Convert to minutes

# Ensure that the travel time is only calculated within the same route
stop_times_routes_stops['travel_time_to_next_stop'] = np.where(
    stop_times_routes_stops['route_id'] == stop_times_routes_stops['route_id'].shift(-1),
    stop_times_routes_stops['travel_time_to_next_stop'],
    np.nan
)

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

# Group by route and stop sequence to calculate the average travel time and distance for each stop
route_stop_avg = stop_times_routes_stops.groupby(['route_id', 'stop_sequence']).agg({
    'stop_id': 'first',
    'stop_name': 'first',
    'stop_lat': 'first',
    'stop_lon': 'first',
    'travel_time_to_next_stop': 'mean',
    'distance_to_next_stop': 'mean'
}).reset_index()

# Ensure no stops are missing before merging
all_stop_ids_before_merge = set(stop_times_routes_stops['stop_id'])

# Merge this average back to the original DataFrame
route_stop_df = pd.merge(route_stop_avg, stop_times_routes_stops[['route_id', 'stop_sequence']], on=['route_id', 'stop_sequence'])

# Check for missing stop_ids after merge
missing_stop_ids_after_merge = all_stop_ids_before_merge - set(route_stop_df['stop_id'])

# Ensure that the last stop in each route has NaN for travel time and distance
route_stop_df['travel_time_to_next_stop'] = np.where(
    route_stop_df['stop_sequence'] == route_stop_df.groupby('route_id')['stop_sequence'].transform('max'),
    np.nan,
    route_stop_df['travel_time_to_next_stop']
)

route_stop_df['distance_to_next_stop'] = np.where(
    route_stop_df['stop_sequence'] == route_stop_df.groupby('route_id')['stop_sequence'].transform('max'),
    np.nan,
    route_stop_df['distance_to_next_stop']
)

# Select only the necessary columns
route_stop_df = route_stop_df[['route_id', 'stop_id', 'stop_name', 'stop_sequence', 'stop_lat', 'stop_lon', 'travel_time_to_next_stop', 'distance_to_next_stop']]

# Remove any potential duplicates
route_stop_df = route_stop_df.drop_duplicates()

# Save the result to a new CSV file
route_stop_df.to_csv('output/route_stop_mapping.csv', index=False)

# Output any missing stop IDs after processing
print(f"Number of missing stop_ids after processing: {len(missing_stop_ids_after_merge)}")
print("Missing stop_ids:", missing_stop_ids_after_merge)

print("Route stop mapping with average travel time, distance, and coordinates has been saved successfully!")
