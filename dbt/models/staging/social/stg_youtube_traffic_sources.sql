{{- config(
  materialized='view'
) -}}

SELECT
  PARSE_DATE('%Y-%m-%d', day) AS date_tzutc
  , channel_id
  , CASE
    WHEN channel_id = "UCs8t-T2D2C2HIXt3VIKHz0A" THEN 'playSIPHER'
    ELSE channel_id
  END AS channel_name
  , insightTrafficSourceType  AS insight_traffic_source_type
  , views
  , estimatedMinutesWatched   AS estimated_minutes_watched
  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'youtube_traffic_sources') }}