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
  , description
  , custom_url
  , CAST(published_at     AS TIMESTAMP) AS published_at
  , CAST(view_count       AS INT64)     AS view_count
  , CAST(subscriber_count AS INT64)     AS subscriber_count
  , hidden_subscriber_count
  , CAST(video_count      AS INT64)     AS video_count
  , privacy_status
  , long_uploads_status
  , made_for_kids
  , self_declared_made_for_kids
  , views
  , likes
  , dislikes
  , shares
  , estimatedMinutesWatched                 AS est_minutes_watched
  , CAST(averageViewDuration   AS FLOAT64)  AS avg_view_duration
  , CAST(averageViewPercentage AS FLOAT64)  AS avg_view_percentage
  , subscribersGained                       AS subscribers_gained
  , subscribersLost                         AS subscribers_lost
  , annotationClickThroughRate              AS annotation_click_through_rate
  , annotationCloseRate                     AS annotation_close_rate
  , annotationImpressions                   AS annotation_impressions
  , __collected_ts
  , snapshot_date
FROM 
  {{ source('raw_social', 'youtube_overview') }}