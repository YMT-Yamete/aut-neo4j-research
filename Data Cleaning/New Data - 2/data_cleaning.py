import pandas as pd
from geopy.distance import geodesic
from tqdm import tqdm

# Load the datasets (update file paths accordingly)
print("Loading datasets...")
stops = pd.read_csv('stops.csv')
stop_times = pd.read_csv('stop_times.csv')
trips = pd.read_csv('trips.csv')
routes = pd.read_csv('routes.csv')
print("Datasets loaded successfully.")

# Merge stop_times with trips to get route_id
print("Merging stop_times with trips...")
stop_times = pd.merge(stop_times, trips[['trip_id', 'route_id']], on='trip_id', how='left')

# Merge stop_times with stops to get stop details
print("Merging stop_times with stops...")
stop_times = pd.merge(stop_times, stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], on='stop_id', how='left')

# Sort by route_id, trip_id, and stop_sequence to ensure the order is correct
print("Sorting data by route_id, trip_id, and stop_sequence...")
stop_times = stop_times.sort_values(by=['route_id', 'trip_id', 'stop_sequence'])

# Identify the first sequence for each route
print("Identifying the first sequence per route...")
first_sequence = stop_times.groupby('route_id')['trip_id'].first().reset_index()
filtered_stop_times = pd.merge(stop_times, first_sequence, on=['route_id', 'trip_id'])

# Calculate travel time to next stop as a float in minutes
print("Calculating travel times to the next stop...")
filtered_stop_times['arrival_time'] = pd.to_datetime(filtered_stop_times['arrival_time'], format='%H:%M:%S', errors='coerce')
filtered_stop_times['departure_time'] = pd.to_datetime(filtered_stop_times['departure_time'], format='%H:%M:%S', errors='coerce')
filtered_stop_times['travel_time_to_next_stop'] = (
    (filtered_stop_times.groupby(['route_id', 'trip_id'])['departure_time'].shift(-1) - filtered_stop_times['arrival_time'])
    .dt.total_seconds() / 60  # Convert to minutes as a float
)

# Calculate distance to next stop with progress tracking
print("Calculating distances to the next stop...")
distance_to_next_stop = []

for (route_id, trip_id), group in tqdm(filtered_stop_times.groupby(['route_id', 'trip_id']), desc="Processing routes", unit="route"):
    group = group.reset_index(drop=True)
    distances = []
    for i in range(len(group) - 1):
        coords_1 = (group.loc[i, 'stop_lat'], group.loc[i, 'stop_lon'])
        coords_2 = (group.loc[i + 1, 'stop_lat'], group.loc[i + 1, 'stop_lon'])
        distances.append(geodesic(coords_1, coords_2).kilometers)
    distances.append(None)  # No next stop for the last stop in the sequence
    distance_to_next_stop.extend(distances)

filtered_stop_times['distance_to_next_stop'] = distance_to_next_stop

# Create final DataFrame with required columns
print("Creating final DataFrame...")
final_df = filtered_stop_times[['route_id', 'stop_id', 'stop_name', 'stop_sequence', 'stop_lat', 'stop_lon', 'travel_time_to_next_stop', 'distance_to_next_stop']]

# Save to CSV
output_file = 'output_stops_with_travel_time_distance_one_sequence_per_route.csv'
final_df.to_csv(output_file, index=False)
print(f"CSV file '{output_file}' generated successfully.")
