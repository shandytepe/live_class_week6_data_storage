select *
from {{ source("pacflight", "bookings") }}