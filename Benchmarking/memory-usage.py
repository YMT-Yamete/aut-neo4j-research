import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# Data for memory usage across different graph sizes
memory_usage_data = {
    'Query': ['Query 1', 'Query 2', 'Query 3', 'Query 4'],
    'Small Graph': [0.02, 0.01, 0.01, 0.01],
    'Medium Graph': [0.02, 0.02, 0.02, 0.01],
    'Large Graph': [0.03, 0.03, 0.02, 0.01]
}

# Create DataFrame
memory_usage_df = pd.DataFrame(memory_usage_data)

# Create multi-group bar chart for Memory Usage
def plot_memory_usage_bar_chart(df):
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
    ax.set_ylabel('Memory Usage (MB)')
    ax.set_title('Memory Usage Across Different Graph Sizes')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(title='Graph Size')

    plt.tight_layout()
    plt.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.show()

# Plot the memory usage bar chart
plot_memory_usage_bar_chart(memory_usage_df)
