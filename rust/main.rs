use anyhow::Result;
use glob::glob;
use polars::prelude::*;
use std::time::Instant;

fn main() -> Result<()> {
    let t0 = Instant::now();

    //Load
    let pattern = r"../Dataset/NYC Taxi Trip/yellow_tripdata_2020-06.csv";
    let zones = LazyCsvReader::new(r"../Dataset/NYC Taxi Trip/taxi+_zone_lookup.csv")
        .with_has_header(true)
        .finish()?;

    println!("CWD: {}", std::env::current_dir()?.display());
    println!("Pattern: {}", pattern);

    let mut count = 0usize;

    for entry in glob(pattern)? {
        match entry {
            Ok(p) => {
                println!("FOUND: {}", p.display());
                count += 1;
            }
            Err(e) => println!("GLOB ERR: {}", e),
        }
    }

    println!("Matched files: {}", count);
    let mut lazy_frames: Vec<LazyFrame> = Vec::new();

    for entry in glob(pattern)? {
        let path = entry?;

        let lf = LazyCsvReader::new(path)
            .with_has_header(true)
            .finish()?
            .select([
                col("tpep_pickup_datetime"),
                col("tpep_dropoff_datetime"),
                col("trip_distance"),
                col("fare_amount"),
                col("PULocationID"),
            ]);

        lazy_frames.push(lf);
    }

    let df = concat(lazy_frames, UnionArgs::default())?;
    println!("Loaded files in {:?}", t0.elapsed());

    // -----------------------------
    // Load zone lookup
    // -----------------------------

    // -----------------------------
    // Transform pipeline
    // -----------------------------
    let df = df
        // Parse datetime
        .with_columns([
            col("tpep_pickup_datetime")
                .str()
                .strptime(
                    DataType::Datetime(TimeUnit::Milliseconds, None),
                    StrptimeOptions {
                        strict: false,
                        ..Default::default()
                    },
                    lit("raise"),
                )
                .alias("pickup_dt"),
            col("tpep_dropoff_datetime")
                .str()
                .strptime(
                    DataType::Datetime(TimeUnit::Milliseconds, None),
                    StrptimeOptions {
                        strict: false,
                        ..Default::default()
                    },
                    lit("raise"),
                )
                .alias("dropoff_dt"),
        ])
        // Feature engineering
        .with_columns([
            (col("dropoff_dt") - col("pickup_dt"))
                .dt()
                .total_minutes()
                .alias("trip_duration"),
            (col("fare_amount") / col("trip_distance")).alias("fare_per_km"),
        ])
        // Filtering
        .filter(
            col("trip_distance")
                .gt(lit(0))
                .and(col("fare_amount").gt(lit(0)))
                .and(col("trip_duration").gt(lit(0))),
        )
        // Extract hour
        .with_columns([col("pickup_dt").dt().hour().alias("hour")])
        // Peak hour filter (no is_in)
        .filter(
            col("hour")
                .eq(lit(7))
                .or(col("hour").eq(lit(8)))
                .or(col("hour").eq(lit(9)))
                .or(col("hour").eq(lit(17)))
                .or(col("hour").eq(lit(18)))
                .or(col("hour").eq(lit(19))),
        )
        // Join
        .join(
            zones,
            [col("PULocationID")],
            [col("LocationID")],
            JoinArgs::new(JoinType::Left),
        )
        // Aggregation
        .group_by([col("Borough")])
        .agg([
            col("fare_amount").mean().alias("avg_fare"),
            col("trip_duration").mean().alias("avg_duration"),
            col("fare_amount").count().alias("trip_count"),
        ]);

    // -----------------------------
    // Execute
    // -----------------------------
    let t1 = Instant::now();
    let result = df.collect()?;
    println!("Execution time: {:?}", t1.elapsed());

    // -----------------------------
    // Write output
    // -----------------------------
    let t2 = Instant::now();

    let mut file = std::fs::File::create("output.parquet")?;
    let mut result_mut = result.clone();
    ParquetWriter::new(&mut file).finish(&mut result_mut)?;

    println!("Write time: {:?}", t2.elapsed());
    println!("TOTAL time: {:?}", t0.elapsed());

    Ok(())
}
