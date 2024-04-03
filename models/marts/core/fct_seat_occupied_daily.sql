with stg_flights as (
    select *
    from {{ ref("stg_pacflight__flights") }}
),

dim_dates as (
    select *
    from {{ ref("dim_date") }}
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

stg_seats as (
    select *
    from {{ ref("stg_pacflight__seats") }}
),

cnt_seat_occupied as (
    select
        sf.flight_id,
        count(seat_no) as seat_occupied
    from stg_flights sf
    join stg_boarding_passes sbp 
        on sbp.flight_id = sf.flight_id
    where
        status = 'Arrived'
    group by 1
),

cnt_total_seats as (
    select
        aircraft_code,
        count(seat_no) as total_seat
    from stg_seats
    group by 1
),

final_fct_seat_occupied_daily as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["sf.flight_id", "sf.flight_no"] ) }} as sk_seat_occupied_daily,
        dd.date_id as date_flight,
        sf.flight_id as nk_flight_id,
        sf.flight_no,
        da1.sk_airport_code as departure_airport,
        da2.sk_airport_code as arrival_airport,
        sf.status,
        dac.sk_aircraft_code as aircraft_code,
        cts.total_seat,
        cso.seat_occupied,
        (cts.total_seat - cso.seat_occupied) as empty_seats,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at        
    from stg_flights sf
    join dim_dates dd 
        on dd.date_actual = DATE(sf.actual_departure)
    join dim_airports da1
        on da1.nk_airport_code = sf.departure_airport
    join dim_airports da2
        on da2.nk_airport_code = sf.arrival_airport
    join dim_aircrafts dac
        on dac.nk_aircraft_code = sf.aircraft_code
    join cnt_seat_occupied cso
        on cso.flight_id = sf.flight_id
    join cnt_total_seats cts 
        on cts.aircraft_code = sf.aircraft_code
)

select * from final_fct_seat_occupied_daily