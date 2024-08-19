import pandas as pd

# Load the CSV files
stops = pd.read_csv('stops.csv').drop_duplicates(subset=['stop_id'])
route_stop_mapping = pd.read_csv('route_stop_mapping.csv').drop_duplicates(subset=['stop_id'])

# Find stop_ids that are in stops.csv but not in route_stop_mapping.csv
extra_stop_ids = set(stops['stop_id']) - set(route_stop_mapping['stop_id'])

# Convert the extra stop_ids to a DataFrame for easier output
extra_stop_ids_df = pd.DataFrame(list(extra_stop_ids), columns=['extra_stop_id'])

# Save the result to a CSV file
extra_stop_ids_df.to_csv('extra_stop_ids.csv', index=False)

# Optionally, display the first few extra stop_ids
print(extra_stop_ids_df.head())
