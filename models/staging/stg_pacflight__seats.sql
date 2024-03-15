select *
from {{ source("pacflight", "seats") }}