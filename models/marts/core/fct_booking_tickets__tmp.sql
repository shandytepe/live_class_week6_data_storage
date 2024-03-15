{{
    config(
        materialized="view"
    )
}}

with stg_fct_booking_tickets as (
    select 
        book_ref as nk_book,
        book_date,
        total_amount
    from {{ ref("stg_pacflight__bookings") }}
),

dim_times as (
    select *
    from {{ ref("dim_time") }}
),

dim_dates as (
    select *
    from {{ ref("dim_date") }}
),

stg_tickets as (
    select *
    from {{ ref("stg_pacflight__tickets") }}
),

dim_passengers as (
    select *
    from {{ ref("dim_passengers") }}
),

tmp_tickets as (
    select 
        st.ticket_no,
        dp.sk_passenger_id
    from stg_tickets st 
    join dim_passengers dp
        on dp.nk_passenger_id = st.passenger_id
),

final_fct_booking_tickets as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["sfbt.nk_book"] ) }} as sk_booking_ticket_id,
        sfbt.nk_book,
        dd1.date_id as book_date_local,
        dd2.date_id as book_date_utc,
        dt1.time_id as book_time_local,
        dt2.time_id as book_time_utc,
        sfbt.total_amount
    from stg_fct_booking_tickets sfbt
    join dim_dates dd1
        on dd1.date_actual = DATE(sfbt.book_date)
    join dim_dates dd2
        on dd2.date_actual = DATE(sfbt.book_date AT TIME ZONE 'UTC')
    join dim_times dt1
        on dt1.time_actual::time = sfbt.book_date::time
    join dim_times dt2
        on dt2.time_actual::time = (sfbt.book_date AT TIME ZONE 'UTC')::time
),

tmp_test as (
    select
        ffbt.*,
        tt.*
    from final_fct_booking_tickets ffbt, tmp_tickets tt
)

select * from tmp_test