{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','yellow') }}
  where vendorid is not null AND 
        date_part('year', pickup_datetime) = 2020
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'pickup_datetime']) }} as tripid,
    unique_row_id,
    filename,
    {{ safe_integer_cast('vendorid') }} as vendorid,
    {{ safe_integer_cast('ratecodeid') }} as ratecodeid,
    {{ safe_integer_cast('pickup_location_id') }} as pickup_locationid,
    {{ safe_integer_cast('dropoff_location_id') }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    {{ safe_integer_cast('passenger_count') }} as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    {{ safe_integer_cast('trip_type') }} as trip_type,

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,
    coalesce({{ safe_integer_cast('payment_type') }}, 0) as payment_type,
    {{ get_payment_type_description("payment_type") }} as payment_type_description
from tripdata


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}