select * 
from {{ source("pacflight", "aircrafts_data") }}