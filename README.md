#High-Performance Data Transformation Engine
A comparative systems-level project that benchmarks data processing performance between Python (pandas) and Rust (Polars) using large-scale real-world dataset.

#Overview
This project implements a data transformation pipeline that:

    Reads large CSV datasets (NYC Taxi data)
    Performs feature engineering
    Applies filtering and aggregations
    Joins with lookup tables
    Writes results to Parquet

Two implementations are built:

    1. Python -> pandas (baseline)
    2. Rust -> Polars (parallel, optimized)

#Pipeline Architecture
CSV Files → Load → Transform → Filter → Join → Aggregate → Parquet Output

#Operations:
    Datetime parsing
    Feature engineering (trip_duration, fare_per_km)
    Peak-hour filtering
    GroupBy aggregations (by Borough)
    Join with zone lookup table

#Dataset
NYC Taxi Trip Dataset
Multi-file partitioned CSVs
Millions of rows per file

#Benchmark Results

Python

    1. Total Runtime: ~3.43s
    2. Memory usage: 125-160MB

Rust

    1. Total Runtime: ~0.48s
    2. Memory usage: Estimated to be ~30-50% of pandas

Rust is ~7x times faster

#Tech Stack
Python
    pandas  
    pyarrow
    psutil
Rust
    Polars (Lazy API)
    glob
    anyhow

#Dataset
From Kaggle: https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019
