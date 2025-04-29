#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 26 09:07:43 2025

@author: michaeljames
"""

import pandas as pd
import os

# List your file names here
file_paths = [
    "spy-price-forecast (26).csv",
    "spy-price-forecast (30).csv",
    "spy-price-forecast (31).csv"
]

# Function to extract parameter metadata from end of file
def extract_parameters(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        meta_lines = lines[-5:]
        params = {}
        for line in meta_lines:
            key, value = line.strip().split(',')
            params[key] = float(value) if key not in ['Forecast Days', 'Backtest Days'] else int(value)
        return params

# Helper function to format the column names
def format_label(prefix, params):
    return f"{prefix} (α={params['Alpha']}, β={params['Beta']}, γ={params['Gamma']}, F={params['Forecast Days']}, B={params['Backtest Days']})"

# Main logic to combine files
combined_df = None
for file_path in file_paths:
    params = extract_parameters(file_path)
    df = pd.read_csv(file_path, skipfooter=5, engine='python')
    if combined_df is None:
        combined_df = pd.DataFrame({"Date": df["Date"]})
    combined_df[format_label("Forecast", params)] = df["Forecast Price"]
    combined_df[format_label("Backtest", params)] = df["Backtest Price"]

# Export the final combined file
combined_df.to_csv("combined_forecast_backtest.csv", index=False)
print("✅ Combined CSV created: combined_forecast_backtest.csv")
