with stg_tickets as (
    select * 
    from {{ ref("stg_pacflight__tickets") }}
),

dim_passengers as (
    select *
    from {{ ref("dim_passengers") }}
),

stg_ticket_flights as (
    select *
    from {{ ref("stg_pacflight__ticket_flights") }}
),

stg_flights as (
    select *
    from {{ ref("stg_pacflight__flights") }}
),

dim_dates as (
    select *
    from {{ ref("dim_date") }}
),

dim_times as (
    select *
    from {{ ref("dim_time") }}
),

dim_airports as (
    select *
    from {{ ref("dim_airports") }}
),

dim_aircrafts as (
    select *
    from {{ ref("dim_aircrafts") }}
),

stg_boarding_passes as (
    select *
    from {{ ref("stg_pacflight__boarding_passes") }}
),

final_fct_boarding_pass as (
    select 
        {{ dbt_utils.generate_surrogate_key ( ["st.ticket_no", "st.book_ref", "st.passenger_id"] ) }} as sk_boarding_pass_id,
        st.ticket_no,
        st.book_ref,
        dp.sk_passenger_id,
        stf.flight_id,
        stf.fare_conditions,
        sf.flight_no,
        dd1.date_id as scheduled_departure_date_local,
        dd2.date_id as scheduled_departure_date_utc,
        dt1.time_id as scheduled_departure_time_local,
        dt2.time_id as scheduled_departure_time_utc,
        dd3.date_id as scheduled_arrival_date_local,
        dd4.date_id as scheduled_arrival_date_utc,
        dt3.time_id as scheduled_arrival_time_local,
        dt4.time_id as scheduled_arrival_time_utc,
        da1.sk_airport_code as departure_airport,
        da2.sk_airport_code as arrival_airport,
        sf.status,
        dac.sk_aircraft_code as aircraft_code,
        sbp.boarding_no,
        sbp.seat_no,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_tickets st
    join dim_passengers dp
        on dp.nk_passenger_id = st.passenger_id
    join stg_ticket_flights stf 
        on stf.ticket_no = st.ticket_no
    join stg_flights sf 
        on sf.flight_id = stf.flight_id
    join dim_dates dd1 
        on dd1.date_actual = DATE(sf.scheduled_departure)
    join dim_dates dd2
        on dd2.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
    join dim_times dt1
        on dt1.time_actual::time = (sf.scheduled_departure)::time
    join dim_times dt2
        on dt2.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
    join dim_dates dd3
        on dd3.date_actual = DATE(sf.scheduled_arrival)
    join dim_dates dd4
        on dd4.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
    join dim_times dt3
        on dt3.time_actual::time = (sf.scheduled_arrival)::time
    join dim_times dt4
        on dt4.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
    join dim_airports da1 
        on da1.nk_airport_code = sf.departure_airport
    join dim_airports da2
        on da2.nk_airport_code = sf.arrival_airport
    join dim_aircrafts dac
        on dac.nk_aircraft_code = sf.aircraft_code
    join stg_boarding_passes sbp
        on sbp.flight_id = stf.flight_id
        and sbp.ticket_no = stf.ticket_no
)

select * from final_fct_boarding_pass