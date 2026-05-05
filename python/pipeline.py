import pandas as pd
import time
import psutil
import os

def log(stage, start_time):
    elapsed = time.time() - start_time
    process = psutil.Process(os.getpid())
    mem_mb=process.memory_info().rss / (1024 ** 2)
    print(f"({stage:20s} | time: {elapsed:8.2f}s) | mem:{mem_mb:8.2f} MB")
    

INPUT_PATH="Dataset/NYC Taxi Trip/yellow_tripdata_2020-06.csv"
OUTPUT_PATH="Dataset/NYC Taxi Trip/yellow_tripdata_2020-06.parquet"


#reading
t0=time.time()

df=pd.read_csv(
    INPUT_PATH,
    parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
    usecols=[
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
    ]
)
log("read_csv", t0)

#feature engineering
t1=time.time()

df["trip_duration"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60 #this is in minutes
df["fare_per_km"] = df["fare_amount"] / df["trip_distance"]
log("feature_engineering", t1)

#filtering
t2=time.time()
df=df[
    (df["trip_distance"] > 0) &
    (df["fare_amount"] > 0) &
    (df["trip_duration"] > 0)]

#peakhours
df["hour"] = df["tpep_pickup_datetime"].dt.hour
df = df[df["hour"].isin([7,8,9,17,18,19])]
log("filtering", t2)

#aggregation
t3=time.time()
agg=df.groupby("PULocationID").agg(
    avg_fare=("fare_amount", "mean"),
    avg_duration=("trip_duration", "mean"),
    trip_count=("fare_amount", "count")
).reset_index()
log("aggregation", t3)

#writing
t4=time.time()
agg.to_parquet(OUTPUT_PATH, engine="pyarrow", index=False)
log("write_parquet", t4)

#total time
log("Total:",t0)


