SELECT time_played as time_start, 
platform,
 track_name,
  artist_name,
 album_name,
 reason_start,
 reason_end,
 shuffle,
 skipped
 from {{ref('stage')}}
 where track_name is not null

