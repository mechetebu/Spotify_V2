with source as (
        select * from streaming_history
  ),
  renamed as (
      select
          *

      from source
  )
  select * from renamed
    