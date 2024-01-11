{{- config(
  materialized='table',
  partition_by={
      "field": "date_tzutc",
      "data_type": "date",
      "granularity": "day"
},
) -}}

WITH ov AS (
SELECT
    date_tzutc
    , channel_id
    , channel_name
    , description
    , custom_url
    , published_at
    , privacy_status
    , long_uploads_status
    , made_for_kids
    , self_declared_made_for_kids
    , view_count
    , subscriber_count
    , hidden_subscriber_count
    , subscribers_gained
    , subscribers_lost
    , (subscribers_gained - subscribers_lost) AS subscriber_getted
    , video_count
    , views
    , likes
    , dislikes
    , shares
    , est_minutes_watched
    , avg_view_duration
    , avg_view_percentage
    , annotation_click_through_rate
    , annotation_close_rate
    , annotation_impressions
FROM
    {{ ref('stg_youtube_overview') }}
)

SELECT * FROM ov