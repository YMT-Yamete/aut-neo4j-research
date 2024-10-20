import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Data for execution time across different graph sizes
execution_time_data = {
    'Query': ['Query 1', 'Query 2', 'Query 3', 'Query 4'],
    'Small Graph': [0.24, 0.24, 0.21, 0.21],
    'Medium Graph': [0.25, 0.25, 0.22, 0.22],
    'Large Graph': [0.31, 0.25, 0.23, 0.22]
}

# Create DataFrame
execution_time_df = pd.DataFrame(execution_time_data)

# Create multi-group bar chart for Execution Time
def plot_execution_time_bar_chart(df):
    labels = df['Query']
    x = np.arange(len(labels))  # the label locations
    width = 0.2  # the width of the bars

    fig, ax = plt.subplots(figsize=(10, 6))

    # Create bars for each graph size
    ax.bar(x - width, df['Small Graph'], width, label='Small Graph')
    ax.bar(x, df['Medium Graph'], width, label='Medium Graph')
    ax.bar(x + width, df['Large Graph'], width, label='Large Graph')

    # Add some text for labels, title, and custom x-axis tick labels
    ax.set_xlabel('Queries')
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('Execution Time Across Different Graph Sizes')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(title='Graph Size')

    plt.tight_layout()
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.show()

# Plot the execution time bar chart
plot_execution_time_bar_chart(execution_time_df)
