#!/usr/bin/env python3
"""
Generate a percentiles comparison chart from benchmark results.
"""

import json
import sys
import os
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib
    import numpy as np
    matplotlib.use('Agg')  # Use non-interactive backend
except ImportError:
    print("Error: matplotlib and numpy are required. Install them with: pip install matplotlib numpy", file=sys.stderr)
    sys.exit(1)


def extract_percentiles_from_report(report_file):
    """Extract percentile values from a benchmark report JSON file."""
    try:
        with open(report_file, 'r') as f:
            data = json.load(f)
        
        # The report structure: { "scenario_name": { "Metrics": { "Time": { "P50": ..., "P75": ..., ... } } } }
        # We take the first scenario's percentiles (or average if multiple)
        percentiles = {
            'P50': [],
            'P75': [],
            'P95': [],
            'P99': [],
            'P999': []
        }
        
        for scenario_name, scenario_data in data.items():
            if isinstance(scenario_data, dict) and 'Metrics' in scenario_data:
                metrics = scenario_data['Metrics']
                if isinstance(metrics, dict) and 'Time' in metrics:
                    time_metrics = metrics['Time']
                    if isinstance(time_metrics, dict):
                        for p_key in percentiles.keys():
                            if p_key in time_metrics:
                                # Convert string like "54.114709ms" or "1.697426583s" to float (milliseconds)
                                value_str = time_metrics[p_key]
                                if isinstance(value_str, str):
                                    value_str = value_str.strip()
                                    # Handle different time units
                                    if value_str.endswith('ms'):
                                        value_ms = float(value_str.replace('ms', '').strip())
                                    elif value_str.endswith('s'):
                                        value_ms = float(value_str.replace('s', '').strip()) * 1000
                                    elif value_str.endswith('us'):
                                        value_ms = float(value_str.replace('us', '').strip()) / 1000
                                    elif value_str.endswith('ns'):
                                        value_ms = float(value_str.replace('ns', '').strip()) / 1000000
                                    else:
                                        # Try to parse as float (assuming milliseconds)
                                        value_ms = float(value_str)
                                    percentiles[p_key].append(value_ms)
                                elif isinstance(value_str, (int, float)):
                                    percentiles[p_key].append(float(value_str))
        
        # Average if multiple scenarios
        result = {}
        for p_key, values in percentiles.items():
            if values:
                result[p_key] = sum(values) / len(values)
            else:
                result[p_key] = None
        
        return result if any(result.values()) else None
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Warning: Could not read percentiles from {report_file}: {e}", file=sys.stderr)
        return None


def generate_chart(results_dir, output_file):
    """Generate a bar chart comparing percentiles across drivers."""
    results_path = Path(results_dir)
    
    # Find all report JSON files
    report_files = list(results_path.glob("*-report.json"))
    
    if not report_files:
        print(f"Error: No report files found in {results_dir}", file=sys.stderr)
        return False
    
    # Extract driver names and percentile values
    drivers = []
    percentiles_data = {
        'P50': [],
        'P75': [],
        'P95': [],
        'P99': [],
        'P999': []
    }
    
    for report_file in sorted(report_files):
        # Extract driver name from filename (e.g., "sqlite-mattn-report.json" -> "sqlite-mattn")
        driver_name = report_file.stem.replace("-report", "")
        percentiles = extract_percentiles_from_report(report_file)
        
        if percentiles:
            drivers.append(driver_name)
            for p_key in percentiles_data.keys():
                percentiles_data[p_key].append(percentiles.get(p_key))
    
    if not drivers:
        print("Error: No valid percentile data found in reports", file=sys.stderr)
        return False
    
    # Create the chart
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Set up the bar positions
    x = np.arange(len(drivers))
    width = 0.15  # Width of bars
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6']
    
    # Plot bars for each percentile
    bars = []
    labels = []
    for i, (p_key, values) in enumerate(percentiles_data.items()):
        if any(v is not None for v in values):
            bar = ax.bar(x + i * width, values, width, label=p_key, color=colors[i % len(colors)], alpha=0.8)
            bars.append(bar)
            labels.append(p_key)
    
    # Add value labels on top of bars
    for bar_group in bars:
        for bar in bar_group:
            height = bar.get_height()
            if height is not None and not np.isnan(height):
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}',
                       ha='center', va='bottom', fontsize=8)
    
    # Customize the chart
    ax.set_xlabel('Storage Driver', fontsize=12, fontweight='bold')
    ax.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
    ax.set_title('Latency Percentiles Comparison by Storage Driver', fontsize=14, fontweight='bold')
    ax.set_xticks(x + width * (len(labels) - 1) / 2)
    ax.set_xticklabels(drivers)
    ax.legend(loc='upper left')
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()
    
    # Save the chart
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Chart saved to: {output_path}")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: generate_percentiles_chart.py <results_dir> [output_file]", file=sys.stderr)
        sys.exit(1)
    
    results_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else os.path.join(results_dir, "percentiles-comparison.png")
    
    if not generate_chart(results_dir, output_file):
        sys.exit(1)


if __name__ == "__main__":
    main()

