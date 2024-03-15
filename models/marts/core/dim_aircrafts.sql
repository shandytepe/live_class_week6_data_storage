with stg_dim_aircrafts as (
    select
        aircraft_code as nk_aircraft_code,
        model->>'en' as model,
        "range"
    from {{ ref("stg_pacflight__aircrafts_data") }}
),

final_dim_aircrafts as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_aircraft_code"] ) }} as sk_aircraft_code,
        *,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_dim_aircrafts
)

select * from final_dim_aircrafts