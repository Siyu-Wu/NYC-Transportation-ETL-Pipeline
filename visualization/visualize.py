import geopandas
import pandas as pd
import matplotlib.pyplot as plt

# Load the taxi_zones shapefile
taxi_zones = geopandas.read_file("data/taxi_zones.zip").to_crs("EPSG:4326")

# Read the CSV file containing your Citi Bike data
df = pd.read_csv('data/202302-citibike-tripdata.csv')

# Create GeoDataFrames for both start and end stations
start_gdf = geopandas.GeoDataFrame(
    df,
    crs="EPSG:4326",
    geometry=geopandas.points_from_xy(df["start_lng"], df["start_lat"])
)

end_gdf = geopandas.GeoDataFrame(
    df,
    crs="EPSG:4326",
    geometry=geopandas.points_from_xy(df["end_lng"], df["end_lat"])
)

# Plot the taxi_zones
ax = taxi_zones.plot(figsize=(12, 8), color="lightgray")

# Plot the Citi Bike start station points on top of the taxi_zones
start_gdf.plot(ax=ax, marker='o', color='blue', markersize=3, label="Start Stations")

# Plot the Citi Bike end station points on top of the taxi_zones
end_gdf.plot(ax=ax, marker='x', color='red', markersize=3, label="End Stations")

plt.title("Citi Bike Start and End Stations Over Taxi Zones")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.legend()
plt.show()
