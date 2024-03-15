with stg_dim_passengers as (
    select 
        passenger_id as nk_passenger_id,
        contact_data->>'phone' as phone,
        contact_data->>'email' as email
    from {{ ref("stg_pacflight__tickets") }}
),

final_dim_passengers as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_passenger_id"] ) }} as sk_passenger_id,
        *,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_dim_passengers
)

select * from final_dim_passengers