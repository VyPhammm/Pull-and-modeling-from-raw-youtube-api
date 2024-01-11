{{- config(
  materialized='view'
) -}}

SELECT
    channel
    , ageGroup AS age_group
    , gender
    , viewerPercentage AS viewer_percentage
    , __collected_ts
    , channel_id
    , snapshot_date
FROM 
  {{ source('raw_social', 'youtube_demographics') }}