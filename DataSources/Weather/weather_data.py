import requests
import pandas as pd
import os
from datetime import datetime

# --- Configuration ----------------------------------------------------------
# Approx Center of Great Britain (Lancashire/Yorkshire border)
LAT = 54.5
LON = -2.0

# API Endpoint for Historical Data
API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Output Filename Template
OUTPUT_FILE = "uk_weather_data_{}_{}.csv"

def fetch_weather_raw(start_date, end_date):
    """
    Fetches hourly weather data from Open-Meteo Archive API.
    Returns a Pandas DataFrame.
    """
    print(f"[Fetch] Requesting data from {start_date} to {end_date}...")
    
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date,
        "end_date": end_date,
        # Key variables for Grid Analysis:
        # - wind_speed_100m: Matches typical onshore turbine height
        # - direct_radiation: Good proxy for solar PV output
        # - temperature_2m: Standard measure for heating demand
        "hourly": "temperature_2m,wind_speed_100m,direct_radiation",
        "timezone": "UTC",
        "wind_speed_unit": "kmh" 
    }

    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status() # Raise error for bad status codes
        data = response.json()
        
        # The API returns a dict with 'hourly' containing lists. 
        # Convert this directly to a DataFrame.
        hourly_data = data.get("hourly", {})
        df = pd.DataFrame(hourly_data)
        
        # Rename columns to be more descriptive/safe
        df.rename(columns={
            "time": "Date",
            "temperature_2m": "Temperature_C",
            "wind_speed_100m": "Wind_Speed_100m_kph",
            "direct_radiation": "Solar_Radiation_W_m2"
        }, inplace=True)

        return df

    except requests.exceptions.RequestException as e:
        print(f"[Error] API Request failed: {e}")
        return pd.DataFrame()

def save_to_csv(df, start_date, end_date):
    if df.empty:
        print("[Warn] No data to save.")
        return

    filename = OUTPUT_FILE.format(start_date, end_date)
    
    # Save to CSV
    # index=False because 'Date' is already a column from the API
    df.to_csv(filename, index=False)
    print(f"[Success] Saved {len(df)} rows to: {filename}")
    print(f"          Columns: {list(df.columns)}")

if __name__ == "__main__":
    # --- INPUT SECTION ------------------------------------------------------
    # You can hardcode these or use input().
    # Format MUST be YYYY-MM-DD
    
    print("--- UK National Weather Downloader ---")
    u_start = input("Enter Start Date (YYYY-MM-DD): ").strip()
    u_end   = input("Enter End Date   (YYYY-MM-DD): ").strip()

    # Basic Validation
    if not u_start or not u_end:
        print("Dates cannot be empty.")
    else:
        # 1. Fetch
        df_weather = fetch_weather_raw(u_start, u_end)
        
        # 2. Save
        save_to_csv(df_weather, u_start, u_end)