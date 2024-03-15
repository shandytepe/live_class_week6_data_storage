select *
from {{ source("pacflight", "tickets") }}