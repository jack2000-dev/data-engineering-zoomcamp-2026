with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        cast(dispatching_base_num as string) as dispatching_base_num,
        {{ safe_cast('pulocationid', 'integer') }} as pickup_location_id,
        {{ safe_cast('dolocationid', 'integer') }} as dropoff_location_id,
        {{ safe_cast('sr_flag', 'integer') }} as sr_flag,
        cast(affiliated_base_number as string) as affiliated_base_number,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime

    from source
    where dispatching_base_num is not null
)

select * from renamed

{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}
