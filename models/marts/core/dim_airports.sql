with stg_dim_airports as (
    select
        airport_code as nk_airport_code,
        airport_name->>'en' as airport_name,
        city->>'en' as city,
        coordinates,
        timezone
    from {{ ref("stg_pacflight__airports_data") }}
),

final_dim_airports as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["nk_airport_code"] ) }} as sk_airport_code,
        *,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_dim_airports
)

select * from final_dim_airports