{{- config(
  materialized='table',
  partition_by={
    'field': 'video_id',
    'data_type': 'STRING',
  },
) -}}

with draft AS (
  SELECT
    channel_id
    , channel_title
    , video_id
    , CONCAT('https://www.youtube.com/watch?v=', video_id) AS content_url
    , video_title
    , video_description
    , published_at
    , DATE(published_at) AS published_date
    , position
    , content_details_duration AS duration
    , creator_content_type 
    , category_id
    , live_broadcast_content
    , content_details_dimension
    , content_details_definition
    , content_details_caption
    , content_details_licensed_content
    , content_details_projection
    , content_details_has_custom_thumbnail
    , status_privacy_status
    , status_license
    , status_embeddable
    , status_public_stats_viewable
    , status_made_for_kids
  FROM
    {{ ref('stg_youtube_contents') }}
  GROUP BY 1,2,3,5,6,7,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)
, dim AS (
  SELECT 
  *, ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY position, creator_content_type NULLS LAST) AS row_number
  FROM draft
)

SELECT * EXCEPT(row_number) 
FROM dim 
WHERE row_number = 1
ORDER BY position
