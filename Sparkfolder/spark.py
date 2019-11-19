import pandas as pd

turl = "gs://noaa_weather_data/NOAA_Strom_DataSet/StormEvents_details/StormEvents_Details/StormEvents_details-ftp_v1.0_d1950_c20170120.csv"

df = pd.read_csv(turl)

df1 = df.groupby("STATE").max()

df1 = df.groupby("CITIES").min()

df1 = df.groupby("DEATHS_DIRECT").max()

df1 = df.groupby("EVENT_TYPE").max()

df1 = df.groupby("DAMAGE_PROPERTY").min()

print(df1.head(10))
