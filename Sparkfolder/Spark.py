try:

    from google.cloud import storage
    import pandas as pd
    import os
    from io import StringIO
    import sys
except  Exception as e:
    print("Some Modules are missing {}".format(e))

client = storage.Client()
bucket = client.bucket('noaa_weather_data')
print(bucket)
blobs = list(bucket.list_blobs(prefix="NOAA_Strom_DataSet/StormEvents_details/StormEvents_Details/"))


for c , d in enumerate(blobs):
    print(c, "\t", d)
    if c == 0:
        pass
    else:

        read_output = d.download_as_string()
        s = str(read_output, 'utf-8')
        data = StringIO(s)
        df = pd.read_csv(data)
        df1 = df.groupby(["STATE"]).sum()
        df2 = df.sort_values(by='DEATHS_DIRECT', ascending=True)

        df3= df2.drop(["BEGIN_DATE_TIME", "BEGIN_YEARMONTH", "BEGIN_DAY", "BEGIN_TIME", "END_YEARMONTH", "END_DAY", "END_TIME", "EPISODE_ID", "END_RANGE", "END_AZIMUTH", "END_LOCATION", "WFO", "STATE_FIPS" , "TOR_OTHER_WFO", "TOR_OTHER_CZ_STATE", "BEGIN_LOCATION", "BEGIN_LAT", "BEGIN_LON", "END_LAT","END_LON","CZ_TIMEZONE", "END_DATE_TIME", "MAGNITUDE_TYPE", "FLOOD_CAUSE", "CATEGORY","TOR_F_SCALE", "TOR_LENGTH", "TOR_WIDTH", "SOURCE", "MAGNITUDE", "DAMAGE_CROPS", "DAMAGE_PROPERTY", "INJURIES_INDIRECT", "DEATHS_INDIRECT", "EVENT_ID", "EVENT_TYPE", "CZ_TYPE",
                  "CZ_FIPS", "CZ_NAME" ,'TOR_OTHER_CZ_FIPS', 'TOR_OTHER_CZ_NAME', 'BEGIN_RANGE',
       'BEGIN_AZIMUTH', 'EPISODE_NARRATIVE', 'EVENT_NARRATIVE', 'DATA_SOURCE'], axis=1)

        print(df3.tail(10))

        #filename = "{}.csv".format(c)

        #data_bytes = df3.to_string()
        #my_str_as_bytes = data_bytes.encode()
        #print(type(my_str_as_bytes))

        #blob = bucket.blob('NOAA_Strom_DataSet/StormEvents_details/StormEvents_Details/oooo.csv')
        #blob.upload_from_filename('ttttt.csv')

        break
