#!/usr/bin/env python
# coding: utf-8



import requests
import pandas as pd
import os
base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"





years = [2023, 2024]
stations = [26953, 31688]





for year in years:
    for month in range(1,13):
        for stationID in stations: 
            url = f"{base_url}?format=csv&stationID={stationID}&Year={year}&Day=1&Month={month}&time=LST&timeframe=1&submit=Download+Data"
            print (url)
            response = requests.get(url) #response object is created here.

            if response.ok:

                base_directory = "/Users/elift/Documents/raw_year_ID_data"
                if not os.path.exists(base_directory):
                    os.makedirs(base_directory)

                filename = os.path.join(base_directory, f"{stationID}_{year}_{month:02}.csv")
                print("Full file path:", filename)
                with open(filename, "wb") as f:
                    f.write(response.content)

            





# Combine all downloaded files into one DataFrame
data_dir = "/Users/elift/Documents/raw_year_ID_data"
all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith(".csv")]

dfs = []
for file in all_files:
    df = pd.read_csv(file)
    dfs.append(df)

full_weather_df = pd.concat(dfs, ignore_index=True)
print(df)





df = full_weather_df[[
    'Longitude (x)',
    'Latitude (y)',
    'Station Name',
    'Climate ID',
    'Date/Time (LST)',
    'Temp (°C)'
]].copy()

print(df.isnull().sum())
print(df['Temp (°C)'].describe())
# Make sure datetime is in correct format
df['Date/Time (LST)'] = pd.to_datetime(df['Date/Time (LST)'])

# create day column for grouping
df['date'] = df['Date/Time (LST)'].dt.date

# group by station and day and count number of entries
missing_check = df.groupby(['Station Name', 'date']).size().reset_index(name='hourly_count')

# find any day with less than 24 readings and put them in a df
missing_days = missing_check[missing_check['hourly_count'] < 24]

print(missing_days)







print(" Days with fewer than 24 readings:")
print(missing_days.sort_values(['Station Name', 'date']).reset_index(drop=True)) #to see there is no missing houred day for real






df['Temp (°C)'] = pd.to_numeric(df['Temp (°C)'], errors='coerce') #turns into integers and if cannot(if str) takes it as NaN
df = df.dropna(subset=['Temp (°C)'])#drops the NaN temps.





df = df[(df['Temp (°C)'] > -50) & (df['Temp (°C)'] < 50)]



print(full_weather_df['Station Name'].value_counts())
print(full_weather_df[full_weather_df['Station Name'] == 'TORONTO NORTH YORK'][['Temp (°C)']].head(20))





print(df)





df['date_month'] = df['Date/Time (LST)'].dt.to_period('M')
grouped = df.groupby(['Station Name', 'date_month'])
summary = grouped['Temp (°C)'].agg(['min', 'max', 'mean']).reset_index()


print(summary)





geo_df = pd.read_csv("/Users/elift/Documents/geonames.csv")





# names to uppercase
summary['Station Name'] = summary['Station Name'].str.strip().str.upper()
geo_df['name'] = geo_df['name'].str.strip().str.upper()

# create mapping since there is a single toronto station in genomes csv, im not sure about this method tho
station_map = {
    'TORONTO CITY': 'TORONTO',
    'TORONTO NORTH YORK': 'TORONTO'
}


summary['geo_name'] = summary['Station Name'].map(station_map)


final_df = summary.merge(
    geo_df[['name', 'feature.id', 'map']],
    how='left',
    left_on='geo_name',
    right_on='name'
).drop(columns=['geo_name', 'name'])
print(final_df[['Station Name', 'feature.id', 'map']].drop_duplicates())






print(final_df)
print(final_df.shape)





print(summary['Station Name'].unique())





final_df.to_csv("final_weather_output.csv", index=False)
print("✅ Final CSV file saved as 'final_weather_output.csv'")



#I couldnt find a working way of merging them correctly, since there is no common column directly in geonames and the one we construct. So that, i couldnt proceed correctly after data extraction. 
