select *
from {{ source("pacflight", "airports_data") }}