select * from {{ source('prod', 'green_tripdata') }}
