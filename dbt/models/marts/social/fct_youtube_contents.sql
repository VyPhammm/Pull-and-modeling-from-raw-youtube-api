{{- config(
  materialized='table',
  partition_by={
      "field": "published_date",
      "data_type": "date",
      "granularity": "day"
},
) -}}


WITH data AS (
    SELECT
        latest_date_data
        , channel_id
        , channel_title
        , video_id
        , video_title
        , published_at
        , position
        , content_details_duration
        , creator_content_type
        , video_description
        , category_id
        , total_views
        , views
        , red_views
        , likes
        , dislikes
        , shares
        , comments
        , subscribers_gained
        , subscribers_lost
        , estimated_minutes_watched               AS est_minutes_watched
        , estimated_red_minutes_watched           AS est_red_minutes_watched
        , average_view_duration                   AS avg_view_duration
        , average_view_percentage                 AS avg_view_percentage
        , annotation_impressions
        , annotation_clickable_impressions
        , annotation_clicks
        , annotation_click_through_rate
        , annotation_closable_impressions
        , annotation_closes
        , annotation_close_rate
        , card_impressions
        , card_clicks
        , card_click_rate
        , card_teaser_impressions
        , card_teaser_clicks
        , card_teaser_click_rate
        , videos_added_to_playlists
        , videos_removed_from_playlists
        , live_broadcast_content
        , content_details_dimension AS dimension
        , content_details_definition AS  definition
        , content_details_caption AS  caption
        , content_details_licensed_content AS  licensed_content
        , content_details_projection AS  projection
        , content_details_has_custom_thumbnail AS  has_custom_thumbnail
        , status_upload_status AS upload_status
        , status_privacy_status AS privacy_status
        , status_license AS license
        , status_embeddable AS embeddable
        , status_public_stats_viewable AS public_stats_viewable
        , status_made_for_kids AS made_for_kids
        , status_self_declared_made_for_kids AS self_declared_made_for_kids
    FROM
        {{ ref('stg_youtube_contents') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53
)
, contents AS (
    SELECT
        latest_date_data
        , channel_id
        , channel_title
        , video_id
        , video_title
        , published_at
        , DATE(published_at) AS published_date
        , CONCAT('https://www.youtube.com/watch?v=', video_id) AS content_url
        , position
        , content_details_duration
        , creator_content_type
        , video_description
        , category_id
        , total_views
        , views
        , red_views
        , likes
        , dislikes 
        , shares
        , comments
        , subscribers_gained
        , subscribers_lost
        , (subscribers_gained - subscribers_lost) AS subscriber_getted
        , est_minutes_watched
        , est_red_minutes_watched
        , avg_view_duration
        , avg_view_percentage
        , annotation_impressions
        , annotation_clickable_impressions
        , annotation_clicks
        , annotation_click_through_rate
        , annotation_closable_impressions
        , annotation_closes
        , annotation_close_rate
        , card_impressions
        , card_clicks
        , card_click_rate
        , card_teaser_impressions
        , card_teaser_clicks
        , card_teaser_click_rate
        , videos_added_to_playlists
        , videos_removed_from_playlists
        , live_broadcast_content
        , dimension
        , definition
        , caption
        , licensed_content
        , projection
        , has_custom_thumbnail
        , upload_status
        , privacy_status
        , license
        , embeddable
        , public_stats_viewable
        , made_for_kids
        , self_declared_made_for_kids
    FROM
        data
)

SELECT * FROM contents