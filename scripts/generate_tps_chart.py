#!/usr/bin/env python3
"""
Generate a TPS comparison chart from benchmark results.
"""

import json
import sys
import os
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
except ImportError:
    print("Error: matplotlib is required. Install it with: pip install matplotlib", file=sys.stderr)
    sys.exit(1)


def extract_tps_from_report(report_file):
    """Extract TPS value from a benchmark report JSON file."""
    try:
        with open(report_file, 'r') as f:
            data = json.load(f)
        
        # The report structure: { "scenario_name": { "TPS": value, ... } }
        # We take the first scenario's TPS (or average if multiple)
        tps_values = []
        for scenario_name, scenario_data in data.items():
            if isinstance(scenario_data, dict):
                # Try both 'TPS' (uppercase) and 'tps' (lowercase) for compatibility
                tps_value = scenario_data.get('TPS') or scenario_data.get('tps')
                if tps_value is not None:
                    tps_values.append(tps_value)
        
        if tps_values:
            return sum(tps_values) / len(tps_values)  # Average TPS if multiple scenarios
        return None
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        print(f"Warning: Could not read TPS from {report_file}: {e}", file=sys.stderr)
        return None


def generate_chart(results_dir, output_file):
    """Generate a bar chart comparing TPS across drivers."""
    results_path = Path(results_dir)
    
    # Find all report JSON files
    report_files = list(results_path.glob("*-report.json"))
    
    if not report_files:
        print(f"Error: No report files found in {results_dir}", file=sys.stderr)
        return False
    
    # Extract driver names and TPS values
    drivers = []
    tps_values = []
    
    for report_file in sorted(report_files):
        # Extract driver name from filename (e.g., "sqlite-mattn-report.json" -> "sqlite-mattn")
        driver_name = report_file.stem.replace("-report", "")
        tps = extract_tps_from_report(report_file)
        
        if tps is not None:
            drivers.append(driver_name)
            tps_values.append(tps)
    
    if not drivers:
        print("Error: No valid TPS data found in reports", file=sys.stderr)
        return False
    
    # Create the chart
    plt.figure(figsize=(10, 6))
    bars = plt.bar(drivers, tps_values, color=['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6'][:len(drivers)])
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.xlabel('Storage Driver', fontsize=12, fontweight='bold')
    plt.ylabel('Transactions Per Second (TPS)', fontsize=12, fontweight='bold')
    plt.title('TPS Comparison by Storage Driver', fontsize=14, fontweight='bold')
    plt.grid(axis='y', alpha=0.3, linestyle='--')
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
        print("Usage: generate_tps_chart.py <results_dir> [output_file]", file=sys.stderr)
        sys.exit(1)
    
    results_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else os.path.join(results_dir, "tps-comparison.png")
    
    if not generate_chart(results_dir, output_file):
        sys.exit(1)


if __name__ == "__main__":
    main()

