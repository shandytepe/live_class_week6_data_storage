select *
from {{ source("pacflight", "ticket_flights") }}