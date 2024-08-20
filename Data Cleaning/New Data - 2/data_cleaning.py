import pandas as pd
from geopy.distance import geodesic
from tqdm import tqdm

# Load the datasets (update file paths accordingly)
print("Loading datasets...")
stops = pd.read_csv('stops.csv')
stop_times = pd.read_csv('stop_times.csv')
trips = pd.read_csv('trips.csv')
routes = pd.read_csv('routes.csv')
shapes = pd.read_csv('shapes.csv')
print("Datasets loaded successfully.")

# Merge stop_times with trips to get route_id and shape_id
print("Merging stop_times with trips...")
stop_times = pd.merge(stop_times, trips[['trip_id', 'route_id', 'shape_id']], on='trip_id', how='left')

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

# Calculate the cumulative distance along the route using shape data
print("Calculating distances using shape data...")
def calculate_distance_along_shape(shape_points):
    distances = [0]  # Start with 0 distance at the first point
    for i in range(1, len(shape_points)):
        coords_1 = shape_points[i-1]
        coords_2 = shape_points[i]
        distances.append(distances[-1] + geodesic(coords_1, coords_2).kilometers)
    return distances

# Create a dictionary to store cumulative distances for each shape_id
shape_distances = {}

for shape_id, shape_group in tqdm(shapes.groupby('shape_id'), desc="Processing shapes", unit="shape"):
    shape_points = list(zip(shape_group['shape_pt_lat'], shape_group['shape_pt_lon']))
    shape_distances[shape_id] = calculate_distance_along_shape(shape_points)

# Calculate the distance to the next stop along the shape
print("Calculating distances to the next stop...")
distance_to_next_stop = []

for (route_id, trip_id), group in tqdm(filtered_stop_times.groupby(['route_id', 'trip_id']), desc="Processing routes", unit="route"):
    group = group.reset_index(drop=True)
    for i in range(len(group) - 1):
        shape_id = group.loc[i, 'shape_id']
        stop_1_lat = group.loc[i, 'stop_lat']
        stop_1_lon = group.loc[i, 'stop_lon']
        stop_2_lat = group.loc[i + 1, 'stop_lat']
        stop_2_lon = group.loc[i + 1, 'stop_lon']
        
        shape_points = list(zip(
            shapes[(shapes['shape_id'] == shape_id)]['shape_pt_lat'], 
            shapes[(shapes['shape_id'] == shape_id)]['shape_pt_lon']
        ))
        
        # Filter the shape points that are between the two stops
        start_point = min(shape_points, key=lambda point: geodesic((stop_1_lat, stop_1_lon), point).kilometers)
        end_point = min(shape_points, key=lambda point: geodesic((stop_2_lat, stop_2_lon), point).kilometers)
        
        start_index = shape_points.index(start_point)
        end_index = shape_points.index(end_point)
        
        if start_index < end_index:
            relevant_shape_points = shape_points[start_index:end_index + 1]
        else:
            relevant_shape_points = shape_points[end_index:start_index + 1][::-1]

        cumulative_distances = calculate_distance_along_shape(relevant_shape_points)
        total_distance = cumulative_distances[-1]  # The total distance along the shape between the two stops
        
        distance_to_next_stop.append(total_distance)
    
    distance_to_next_stop.append(None)  # No next stop for the last stop in the sequence

filtered_stop_times['distance_to_next_stop'] = distance_to_next_stop

# Calculate travel time to next stop as a float in minutes
print("Calculating travel times to the next stop...")
filtered_stop_times['arrival_time'] = pd.to_datetime(filtered_stop_times['arrival_time'], format='%H:%M:%S', errors='coerce')
filtered_stop_times['departure_time'] = pd.to_datetime(filtered_stop_times['departure_time'], format='%H:%M:%S', errors='coerce')
filtered_stop_times['travel_time_to_next_stop'] = (
    (filtered_stop_times.groupby(['route_id', 'trip_id'])['departure_time'].shift(-1) - filtered_stop_times['arrival_time'])
    .dt.total_seconds() / 60  # Convert to minutes as a float
)

# Create final DataFrame with required columns
print("Creating final DataFrame...")
final_df = filtered_stop_times[['route_id', 'stop_id', 'stop_name', 'stop_sequence', 'stop_lat', 'stop_lon', 'travel_time_to_next_stop', 'distance_to_next_stop']]

# Save to CSV
output_file = 'output_stops_with_travel_time_distance_one_sequence_per_route.csv'
final_df.to_csv(output_file, index=False)
print(f"CSV file '{output_file}' generated successfully.")
