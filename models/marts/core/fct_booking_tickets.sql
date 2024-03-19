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

stg_ticket_flights as (
    select *
    from {{ ref("stg_pacflight__ticket_flights") }}
),

stg_flights as (
    select *
    from {{ ref("stg_pacflight__flights") }}
),

dim_airports as (
    select * 
    from {{ ref("dim_airports") }}
),

dim_aircrafts as (
    select * 
    from {{ ref("dim_aircrafts") }}
),

final_fct_booking_tickets as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["sfbt.nk_book", "st.ticket_no"] ) }} as sk_booking_ticket_id,
        sfbt.nk_book,
        dd1.date_id as book_date_local,
        dd2.date_id as book_date_utc,
        dt1.time_id as book_time_local,
        dt2.time_id as book_time_utc,
        sfbt.total_amount,
        st.ticket_no,
        dp.sk_passenger_id,
        stf.flight_id as nk_flight_id,
        stf.fare_conditions,
        stf.amount,
        sf.flight_no,
        dd3.date_id as scheduled_departure_date_local,
        dd4.date_id as scheduled_departure_date_utc,
        dt3.time_id as scheduled_departure_time_local,  
        dt4.time_id as scheduled_departure_time_utc, 
        dd5.date_id as scheduled_arrival_date_local, 
        dd6.date_id as scheduled_arrival_date_utc,
        dt5.time_id as scheduled_arrival_time_local,  
        dt6.time_id as scheduled_arrival_time_utc,
        da1.sk_airport_code as departure_airport,
        da2.sk_airport_code as arrival_airport,
        sf.status,
        dac.sk_aircraft_code as aircraft_code,
        dd7.date_id as actual_departure_date_local, 
        dd8.date_id as actual_departure_date_utc,
        dt7.time_id as actual_departure_time_local,  
        dt8.time_id as actual_departure_time_utc,  
        dd9.date_id as actual_arrival_date_local, 
        dd10.date_id as actual_arrival_date_utc, 
        dt9.time_id as actual_arrival_time_local,  
        dt10.time_id as actual_arrival_time_utc,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from stg_fct_booking_tickets sfbt
    join dim_dates dd1
        on dd1.date_actual = DATE(sfbt.book_date)
    join dim_dates dd2
        on dd2.date_actual = DATE(sfbt.book_date AT TIME ZONE 'UTC')
    join dim_times dt1
        on dt1.time_actual::time = sfbt.book_date::time
    join dim_times dt2
        on dt2.time_actual::time = (sfbt.book_date AT TIME ZONE 'UTC')::time
    join stg_tickets st 
        on st.book_ref = sfbt.nk_book
    join dim_passengers dp
        on dp.nk_passenger_id = st.passenger_id
    join stg_ticket_flights stf 
        on stf.ticket_no = st.ticket_no
    join stg_flights sf 
        on sf.flight_id = stf.flight_id
    join dim_dates dd3 
        on dd3.date_actual = DATE(sf.scheduled_departure)
    join dim_dates dd4
        on dd4.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
    join dim_times dt3 
        on dt3.time_actual::time = (sf.scheduled_departure)::time
    join dim_times dt4
        on dt4.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
    join dim_dates dd5 
        on dd5.date_actual = DATE(sf.scheduled_arrival)
    join dim_dates dd6
        on dd6.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
    join dim_times dt5
        on dt5.time_actual::time = (sf.scheduled_arrival)::time
    join dim_times dt6 
        on dt6.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
    join dim_airports da1
        on da1.nk_airport_code = sf.departure_airport
    join dim_airports da2
        on da2.nk_airport_code = sf.arrival_airport
    join dim_aircrafts dac 
        on dac.nk_aircraft_code = sf.aircraft_code
    join dim_dates dd7 
        on dd7.date_actual = DATE(sf.actual_departure)
    join dim_dates dd8
        on dd8.date_actual = DATE(sf.actual_departure AT TIME ZONE 'UTC')
    join dim_times dt7
        on dt7.time_actual::time = (sf.actual_departure)::time
    join dim_times dt8
        on dt8.time_actual::time = (sf.actual_departure AT TIME ZONE 'UTC')::time
    join dim_dates dd9
        on dd9.date_actual = DATE(sf.actual_arrival)
    join dim_dates dd10
        on dd10.date_actual = DATE(sf.actual_arrival AT TIME ZONE 'UTC')
    join dim_times dt9
        on dt9.time_actual::time = (sf.actual_arrival)::time
    join dim_times dt10
        on dt10.time_actual::time = (sf.actual_arrival AT TIME ZONE 'UTC')::time
)

select * from final_fct_booking_tickets