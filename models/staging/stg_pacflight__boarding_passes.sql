select *
from {{ source("pacflight", "boarding_passes") }}