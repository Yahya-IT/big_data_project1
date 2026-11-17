import random
from datetime import datetime, timedelta
import pandas as pd

random.seed(42)

NUM_ROWS = 5000

locations = [
    ("New York", "NY", 40.7128, -74.0060),
    ("Los Angeles", "CA", 34.0522, -118.2437),
    ("Chicago", "IL", 41.8781, -87.6298),
    ("Houston", "TX", 29.7604, -95.3698),
    ("Phoenix", "AZ", 33.4484, -112.0740),
    ("Miami", "FL", 25.7617, -80.1918),
    ("Dallas", "TX", 32.7767, -96.7970),
    ("Atlanta", "GA", 33.7490, -84.3880),
    ("Seattle", "WA", 47.6062, -122.3321),
    ("Boston", "MA", 42.3601, -71.0589),
]

# Weighted towards major cities so geographical charts show clear hotspots
location_weights = [18, 16, 12, 11, 8, 9, 8, 7, 6, 5]

# Includes messy variants ("light rain", "RAIN ", None) for NiFi ETL cleaning
weather_options = [
    "Clear", "Rain", "light rain", "Heavy Rain", "Fog",
    "Snow", "Overcast", "RAIN ", None
]
weather_weights = [35, 18, 8, 7, 8, 6, 10, 4, 4]

road_conditions = [
    "Dry", "Wet", "Icy", "Under Construction",
    "Potholes", "Poor Lighting", "wet ", None
]
road_weights = [45, 25, 6, 8, 6, 6, 2, 2]

contributing_factors = [
    "Speeding",
    "Distracted Driving",
    "Drunk Driving",
    "Weather Conditions",
    "Road Conditions",
    "Running Red Light",
    "Tailgating"
]
factor_weights = [28, 25, 14, 12, 9, 7, 5]

vehicle_types = ["Car", "Motorcycle", "Truck", "Bicycle"]
vehicle_weights = [64, 14, 15, 7]

severities = [1, 2, 3, 4]
severity_weights = [15, 55, 22, 8]

# Realistic hourly distribution (peaks during morning 7-9 AM and evening 4-7 PM rush hours)
hours = list(range(24))
hour_weights = [
    2, 1, 1, 1, 2, 3, 6, 9, 10, 6, 5, 5,
    6, 6, 7, 8, 10, 11, 9, 6, 5, 4, 3, 2
]

start_base = datetime(2022, 1, 1)
end_base = datetime(2026, 11, 15)
days_range = (end_base - start_base).days

rows = []
for i in range(1, NUM_ROWS + 1):
    city, state, base_lat, base_lng = random.choices(locations, weights=location_weights, k=1)[0]
    
    random_day = start_base + timedelta(days=random.randint(0, days_range))
    chosen_hour = random.choices(hours, weights=hour_weights, k=1)[0]
    chosen_minute = random.randint(0, 59)
    chosen_second = random.randint(0, 59)
    timestamp = random_day.replace(hour=chosen_hour, minute=chosen_minute, second=chosen_second)

    weather = random.choices(weather_options, weights=weather_weights, k=1)[0]
    road = random.choices(road_conditions, weights=road_weights, k=1)[0]
    factor = random.choices(contributing_factors, weights=factor_weights, k=1)[0]
    vehicle = random.choices(vehicle_types, weights=vehicle_weights, k=1)[0]
    severity = random.choices(severities, weights=severity_weights, k=1)[0]

    # Inject ~3% dirty rows (missing city or invalid severity) so NiFi has real records to filter/drop
    if random.random() < 0.03:
        city = None

    rows.append({
        "Accident_ID": f"ACC-{100000 + i}",
        "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        "City": city,
        "State": state,
        "Latitude": round(base_lat + random.uniform(-0.15, 0.15), 5),
        "Longitude": round(base_lng + random.uniform(-0.15, 0.15), 5),
        "Severity": severity,
        "Weather_Condition": weather,
        "Road_Condition": road,
        "Contributing_Factor": factor,
        "Vehicle_Type": vehicle
    })

df = pd.DataFrame(rows)
df.to_csv("traffic_accidents_5000.csv", index=False)
print(f"Successfully generated 'traffic_accidents_5000.csv' with {len(df)} rows!")