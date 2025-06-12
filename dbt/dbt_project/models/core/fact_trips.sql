{{ config(
    materialized='table',
) 
}}

WITH green_tripdata
AS (
    SELECT * , 
           'Green' as service_type
    FROM {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata
AS (
    SELECT * , 
           'Yellow' as service_type
    FROM {{ ref('stg_yellow_tripdata') }}
),
trips_unioned 
AS (
    SELECT * FROM green_tripdata
    union all 
    SELECT * FROM yellow_tripdata
), 
dim_zones 
AS (
    SELECT * 
    FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT 
    trips_unioned.tripid,
    trips_unioned.unique_row_id,
    trips_unioned.filename,
    trips_unioned.vendorid,            
    trips_unioned.ratecodeid,  
    trips_unioned.pickup_locationid,
    pickup_zones.borough as pickup_borough,
    pickup_zones.zone as pickup_zone,
    trips_unioned.dropoff_locationid,
    dropoff_zones.borough as dropoff_borough,
    dropoff_zones.zone as dropoff_zone,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
    trips_unioned.store_and_fwd_flag,
    trips_unioned.passenger_count,
    trips_unioned.trip_distance,
    trips_unioned.trip_type,
    trips_unioned.fare_amount, 
    trips_unioned.extra,
    trips_unioned.mta_tax,
    trips_unioned.tip_amount,
    trips_unioned.tolls_amount,
    trips_unioned.ehail_fee,
    trips_unioned.improvement_surcharge,
    trips_unioned.total_amount,
    trips_unioned.congestion_surcharge,
    trips_unioned.payment_type,
    trips_unioned.payment_type_description,
    trips_unioned.service_type
FROM trips_unioned
INNER JOIN dim_zones as pickup_zones
ON trips_unioned.pickup_locationid = pickup_zones.locationid
INNER JOIN dim_zones as dropoff_zones   
ON trips_unioned.dropoff_locationid = dropoff_zones.locationid

