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

# Merge stop_times with trips to get route_id, shape_id, and direction_id
print("Merging stop_times with trips...")
stop_times = pd.merge(stop_times, trips[['trip_id', 'route_id', 'shape_id', 'direction_id']], on='trip_id', how='left')

# Merge stop_times with stops to get stop details
print("Merging stop_times with stops...")
stop_times = pd.merge(stop_times, stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']], on='stop_id', how='left')

# Sort by route_id, trip_id, and stop_sequence to ensure the order is correct
print("Sorting data by route_id, trip_id, and stop_sequence...")
stop_times = stop_times.sort_values(by=['route_id', 'trip_id', 'stop_sequence'])

# Split the data into inbound and outbound based on direction_id
print("Splitting data into inbound and outbound trips...")
inbound_trips = stop_times[stop_times['direction_id'] == 0].copy()
outbound_trips = stop_times[stop_times['direction_id'] == 1].copy()

# Function to process each direction
def process_direction(trips_data):
    # Identify the first sequence for each route
    print("Identifying the first sequence per route...")
    first_sequence = trips_data.groupby('route_id')['trip_id'].first().reset_index()
    filtered_trips = pd.merge(trips_data, first_sequence, on=['route_id', 'trip_id'])

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

    for (route_id, trip_id), group in tqdm(filtered_trips.groupby(['route_id', 'trip_id']), desc="Processing routes", unit="route"):
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

    filtered_trips['distance_to_next_stop'] = distance_to_next_stop

    # Calculate travel time to next stop as a float in minutes
    print("Calculating travel times to the next stop...")
    filtered_trips['arrival_time'] = pd.to_datetime(filtered_trips['arrival_time'], format='%H:%M:%S', errors='coerce')
    filtered_trips['departure_time'] = pd.to_datetime(filtered_trips['departure_time'], format='%H:%M:%S', errors='coerce')
    filtered_trips['travel_time_to_next_stop'] = (
        (filtered_trips.groupby(['route_id', 'trip_id'])['departure_time'].shift(-1) - filtered_trips['arrival_time'])
        .dt.total_seconds() / 60  # Convert to minutes as a float
    )

    return filtered_trips

# Process both directions and save to separate CSV files
print("Processing inbound trips...")
inbound_final_df = process_direction(inbound_trips)
print("Processing outbound trips...")
outbound_final_df = process_direction(outbound_trips)

# Save to CSV
inbound_output_file = 'inbound_stops_with_travel_time_distance.csv'
outbound_output_file = 'outbound_stops_with_travel_time_distance.csv'

inbound_final_df.to_csv(inbound_output_file, index=False)
outbound_final_df.to_csv(outbound_output_file, index=False)

print(f"CSV files '{inbound_output_file}' and '{outbound_output_file}' generated successfully.")
