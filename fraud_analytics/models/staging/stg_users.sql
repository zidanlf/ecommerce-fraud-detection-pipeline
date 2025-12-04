select
    user_id,
    name,
    email,
    cast(created_date as timestamp) as created_at
from {{ source('zidan_finpro', 'users') }}