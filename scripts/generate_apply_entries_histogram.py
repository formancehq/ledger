#!/usr/bin/env python3
"""
Generate histogram charts for ApplyEntries duration from benchmark results.
"""

import json
import sys
import os
from pathlib import Path
from collections import defaultdict

try:
    import matplotlib.pyplot as plt
    import matplotlib
    import numpy as np
    matplotlib.use('Agg')  # Use non-interactive backend
except ImportError:
    print("Error: matplotlib and numpy are required. Install them with: pip install matplotlib numpy", file=sys.stderr)
    sys.exit(1)


def extract_apply_entries_histogram(server_metrics):
    """
    Extract ApplyEntries histogram data from server metrics.
    
    The metrics structure from OpenTelemetry in-memory exporter looks like:
    {
        "ScopeMetrics": [
            {
                "Metrics": [
                    {
                        "Name": "raft.apply_entries.duration",
                        "Data": {
                            "DataPoints": [
                                {
                                    "Bounds": [...],
                                    "BucketCounts": [...],
                                    ...
                                }
                            ]
                        }
                    }
                ]
            }
        ]
    }
    """
    if not isinstance(server_metrics, dict):
        return None
    
    # Look for ScopeMetrics array
    scope_metrics_list = server_metrics.get("ScopeMetrics") or []
    
    # Iterate through all scope metrics
    all_bounds = []
    all_counts = []
    
    for scope_metrics in scope_metrics_list:
        if not isinstance(scope_metrics, dict):
            continue
        
        metrics_list = scope_metrics.get("Metrics") or []
        
        for metric in metrics_list:
            if not isinstance(metric, dict):
                continue
            
            metric_name = metric.get("name") or metric.get("Name")
            if metric_name == "raft.apply_entries.duration":
                # Extract histogram data from DataPoints
                data = metric.get("data") or metric.get("Data") or {}
                data_points = data.get("data_points") or data.get("DataPoints") or []
                
                # Aggregate all data points
                for data_point in data_points:
                    if not isinstance(data_point, dict):
                        continue
                    
                    bounds = data_point.get("bounds") or data_point.get("Bounds")
                    bucket_counts = data_point.get("bucket_counts") or data_point.get("BucketCounts")
                    
                    if bounds and bucket_counts:
                        # Convert to lists if needed
                        if not isinstance(bounds, list):
                            bounds = list(bounds) if hasattr(bounds, '__iter__') else [bounds]
                        if not isinstance(bucket_counts, list):
                            bucket_counts = list(bucket_counts) if hasattr(bucket_counts, '__iter__') else [bucket_counts]
                        
                        # Aggregate bounds and counts
                        if not all_bounds:
                            all_bounds = bounds.copy()
                            all_counts = [0] * len(bounds)
                        
                        # Add counts to corresponding buckets
                        for i, count in enumerate(bucket_counts):
                            if i < len(all_counts):
                                all_counts[i] += count
    
    # If we found data, return it
    if all_bounds and all_counts:
        # Normalize lengths - if counts has one more element than bounds, it's the +Inf bucket
        if len(all_counts) == len(all_bounds) + 1:
            # Remove the last count (usually +Inf bucket)
            all_counts = all_counts[:-1]
        
        if len(all_bounds) > 0 and len(all_counts) > 0:
            return {
                "buckets": all_bounds,
                "counts": all_counts
            }
    
    return None


def extract_histogram_from_report(report_file):
    """Extract ApplyEntries histogram from a benchmark report JSON file."""
    try:
        with open(report_file, 'r') as f:
            data = json.load(f)
        
        # The report structure: { "scenario_name": { "InternalMetrics": { "server_metrics": {...} } } }
        histograms = []
        
        for scenario_name, scenario_data in data.items():
            if isinstance(scenario_data, dict) and "InternalMetrics" in scenario_data:
                internal_metrics = scenario_data["InternalMetrics"]
                if isinstance(internal_metrics, dict) and "server_metrics" in internal_metrics:
                    server_metrics = internal_metrics["server_metrics"]
                    histogram = extract_apply_entries_histogram(server_metrics)
                    if histogram:
                        histograms.append(histogram)
        
        # If multiple scenarios, merge histograms (use the first one with the most data)
        if histograms:
            # Return the histogram with the most buckets
            return max(histograms, key=lambda h: len(h.get("buckets", [])))
        
        return None
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError) as e:
        print(f"Warning: Could not read histogram from {report_file}: {e}", file=sys.stderr)
        return None


def generate_histogram_chart(results_dir, output_file):
    """Generate histogram charts for ApplyEntries duration for each benchmark."""
    results_path = Path(results_dir)
    
    # Find all report JSON files
    report_files = list(results_path.glob("*-report.json"))
    
    if not report_files:
        print(f"Error: No report files found in {results_dir}", file=sys.stderr)
        return False
    
    # Extract driver names and histogram data
    drivers = []
    histograms = []
    
    for report_file in sorted(report_files):
        # Extract driver name from filename (e.g., "sqlite-mattn-report.json" -> "sqlite-mattn")
        driver_name = report_file.stem.replace("-report", "")
        histogram = extract_histogram_from_report(report_file)
        
        if histogram:
            drivers.append(driver_name)
            histograms.append(histogram)
    
    if not drivers:
        print("Error: No valid ApplyEntries histogram data found in reports", file=sys.stderr)
        return False
    
    # Create a single figure for all drivers (superimposed)
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Define colors for each driver
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6', '#1abc9c', '#34495e']
    
    # Plot histogram for each driver (superimposed)
    for idx, (driver_name, histogram) in enumerate(zip(drivers, histograms)):
        original_buckets = histogram["buckets"]
        original_counts = histogram["counts"]
        
        # Filter out buckets with zero counts for plotting
        buckets = []
        counts = []
        for bucket, count in zip(original_buckets, original_counts):
            if count > 0:
                buckets.append(bucket)
                counts.append(count)
        
        # Convert buckets to bin edges for histogram plotting
        # Use original buckets structure to create proper bin edges
        if len(buckets) > 0 and len(counts) > 0 and len(original_buckets) > 0:
            # Create bin edges from original buckets structure
            # Original buckets are boundaries, we need to create edges for each bucket
            if len(original_buckets) > 1:
                # Calculate bin width from original buckets
                bin_width = original_buckets[1] - original_buckets[0]
                # Create bin edges: each bucket boundary becomes an edge
                bin_edges = original_buckets.copy()
                # Add final edge
                bin_edges.append(original_buckets[-1] + bin_width)
            else:
                # Single bucket case
                bin_width = original_buckets[0] if original_buckets[0] > 0 else 1
                bin_edges = [0, original_buckets[0] + bin_width]
            
            # For each filtered bucket, find its corresponding bin center and width
            bin_centers = []
            bin_widths = []
            filtered_counts = []
            
            for bucket, count in zip(buckets, counts):
                # Find the index of this bucket in original_buckets
                try:
                    bucket_idx = original_buckets.index(bucket)
                except ValueError:
                    # Bucket not found, skip
                    continue
                
                # Calculate bin center and width
                if bucket_idx < len(bin_edges) - 1:
                    bin_center = (bin_edges[bucket_idx] + bin_edges[bucket_idx + 1]) / 2
                    bin_width_val = bin_edges[bucket_idx + 1] - bin_edges[bucket_idx]
                    bin_centers.append(bin_center)
                    bin_widths.append(bin_width_val)
                    filtered_counts.append(count)
            
            # Plot histogram using bar chart with transparency for overlay
            if bin_centers:
                color = colors[idx % len(colors)]
                ax.bar(bin_centers, filtered_counts, width=bin_widths, 
                      alpha=0.6, edgecolor='black', linewidth=0.5, 
                      label=driver_name, color=color)
                
                # Add statistics for this driver
                if filtered_counts:
                    total_count = sum(filtered_counts)
                    if total_count > 0:
                        # Calculate weighted mean using bin centers
                        weighted_sum = sum(center * count for center, count in zip(bin_centers, filtered_counts))
                        mean = weighted_sum / total_count
                        
                        # Find median (50th percentile) - find the bin where cumulative count reaches 50%
                        cumulative = 0
                        median = bin_centers[-1] if bin_centers else 0
                        for center, count in zip(bin_centers, filtered_counts):
                            cumulative += count
                            if cumulative >= total_count / 2:
                                median = center
                                break
                        
                        # Calculate min and max from actual data
                        min_val = min(buckets) if buckets else 0
                        max_val = max(buckets) if buckets else 0
                        
                        # Add statistics text box for each driver (positioned differently)
                        y_pos = 0.95 - (idx * 0.12)  # Stack statistics boxes vertically
                        stats_text = f'{driver_name}:\n  Total: {total_count}\n  Mean: {mean:.2f}ms\n  Median: {median:.2f}ms'
                        ax.text(0.02, y_pos, stats_text, transform=ax.transAxes,
                               verticalalignment='top', horizontalalignment='left',
                               bbox=dict(boxstyle='round', facecolor=colors[idx % len(colors)], alpha=0.3, edgecolor='black'),
                               fontsize=9)
    
    # Set labels and title (only once, after all plots)
    ax.set_xlabel('Duration (ms)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Count', fontsize=12, fontweight='bold')
    ax.set_title('ApplyEntries Duration Histogram Comparison', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.legend(loc='upper right', fontsize=10)
    
    plt.tight_layout()
    
    # Save the chart
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    print(f"Histogram chart saved to: {output_path}")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: generate_apply_entries_histogram.py <results_dir> [output_file]", file=sys.stderr)
        sys.exit(1)
    
    results_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else os.path.join(results_dir, "apply-entries-histogram.png")
    
    if not generate_histogram_chart(results_dir, output_file):
        sys.exit(1)


if __name__ == "__main__":
    main()

