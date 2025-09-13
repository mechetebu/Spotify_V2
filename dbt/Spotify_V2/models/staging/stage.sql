SELECT
strftime(ts, '%Y-%m-%d %I:%M %p') as stream_date,
CASE
  WHEN lower(platform) like '%windows%' THEN 'WINDOWS'
  WHEN lower(platform) like '%ios%' THEN 'IOS'
  when lower(platform) like '%web_player%' THEN 'WEB PLAYER'
  when lower(platform) like '%android%' THEN 'ANDROID'
  when lower(platform) like '%os x%' THEN 'MAC'
  else 'OTHER'
END as platform,
    printf('%02d:%02d', 
        FLOOR(ms_played / 60000)::INTEGER, 
        FLOOR((ms_played % 60000) / 1000)::INTEGER
        ) as time_played,
conn_country as country,
master_metadata_track_name as track_name,
 master_metadata_album_artist_name as artist_name,
 master_metadata_album_album_name as album_name,
 CASE
WHEN reason_start = 'appload' then 'App Started'
WHEN reason_start = 'backbtn' THEN 'Back Button'
WHEN reason_start = 'clickrow' THEN 'Track Selected'
WHEN reason_start = 'fwdbtn' THEN 'Forward Button'
WHEN reason_start = 'playbtn' THEN 'Play Button'
WHEN reason_start = 'remote' THEN 'Remote Control'
WHEN reason_start = 'trackdone' THEN 'Previous track ended'
WHEN reason_start = 'trackerror' THEN 'Track error'
else reason_start
end as reason_start,
CASE
WHEN reason_end = 'logout' then 'User logged out'
WHEN reason_end = 'backbtn' THEN 'Back Button'
WHEN reason_end = 'unexpected-exit-while-paused' THEN 'App crashed / closed while paused'
WHEN reason_end = 'unexpected-exit' THEN 'App crashed / closed'
WHEN reason_end = 'fwdbtn' THEN 'Skipped'
WHEN reason_end = 'playbtn' THEN 'Play Button'
WHEN reason_end = 'remote' THEN 'Remote Control'
WHEN reason_end = 'endplay' THEN 'Played to Completion'
WHEN reason_end = 'trackdone' THEN 'Track completed'
WHEN reason_end = 'trackerror' THEN 'Track error'
else reason_end 
end as reason_end,
episode_name, 
episode_show_name,
audiobook_title,
audiobook_chapter_title,
shuffle,
skipped,
offline,
strftime(to_timestamp(offline_timestamp) , '%Y-%m-%d %I:%M %p') as offline_timestamp

from {{ref('streaming_history')}}