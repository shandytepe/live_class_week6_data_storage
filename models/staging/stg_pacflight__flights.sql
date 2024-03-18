select *
from {{ source("pacflight", "flights") }}