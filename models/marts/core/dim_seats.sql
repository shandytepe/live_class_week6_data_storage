with stg_dim_seats as (
    select
        aircraft_code as nk_aircraft_code,
        seat_no,
        fare_conditions
    from {{ ref("stg_pacflight__seats") }}
),

dim_aircrafts as (
    select *
    from {{ ref("dim_aircrafts") }}
),

final_dim_seats as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["sds.seat_no"] ) }} as sk_seat_id,
        da.sk_aircraft_code,
        sds.seat_no,
        sds.fare_conditions,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_dim_seats sds 
    join dim_aircrafts da 
        on da.nk_aircraft_code = sds.nk_aircraft_code
)

select * from final_dim_seats