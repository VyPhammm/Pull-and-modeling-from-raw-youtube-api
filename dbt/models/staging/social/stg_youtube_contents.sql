{{- config(
  materialized='view'
) -}}

SELECT
  videoId         AS video_id
  , snippet_title AS video_title
  , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', snippet_publishedAt) AS published_at
  , CASE
      WHEN day IS NULL AND views_x IS NOT NULL THEN DATE_SUB(snapshot_date, INTERVAL 3 DAY)
      ELSE PARSE_DATE('%Y-%m-%d', day)
  END AS latest_date_data
  , position 
  , ( SELECT PARSE_TIME('%H:%M:%S', STRING_AGG(IFNULL(LPAD(val, 2, '0'), '00'), ':' ORDER BY OFFSET))
    FROM UNNEST([
      REGEXP_EXTRACT(contentDetails_duration, r'(\d+)H'), 
      REGEXP_EXTRACT(contentDetails_duration, r'(\d+)M'), 
      REGEXP_EXTRACT(contentDetails_duration, r'(\d+)S')
    ]) VAL WITH OFFSET) AS content_details_duration
  , creatorContentType  AS creator_content_type
  , kind
  , snippet_channelId     AS channel_id
  , snippet_channelTitle  AS channel_title
  , snippet_description   AS video_description
  , snippet_categoryId    AS category_id
  , CAST(views_x  AS INT64)  AS total_views
  , CAST(views_y  AS INT64)  AS views
  , CAST(redViews AS INT64)  AS red_views
  , CAST(likes    AS INT64)  AS likes
  , CAST(dislikes AS INT64)  AS dislikes
  , CAST(shares   AS INT64)  AS shares
  , CAST(comments AS INT64)  AS comments
  , CAST(subscribersGained AS INT64)  AS subscribers_gained
  , CAST(subscribersLost   AS INT64)  AS subscribers_lost
  , CAST(videosAddedToPlaylists       AS INT64)     AS videos_added_to_playlists
  , CAST(videosRemovedFromPlaylists   AS INT64)     AS videos_removed_from_playlists
  , estimatedMinutesWatched           AS estimated_minutes_watched
  , estimatedRedMinutesWatched        AS estimated_red_minutes_watched
  , averageViewDuration               AS average_view_duration
  , averageViewPercentage             AS average_view_percentage
  , annotationImpressions             AS annotation_impressions
  , annotationClickableImpressions    AS annotation_clickable_impressions
  , annotationClicks                  AS annotation_clicks
  , annotationClickThroughRate        AS annotation_click_through_rate
  , annotationClosableImpressions     AS annotation_closable_impressions
  , annotationCloses                  AS annotation_closes
  , annotationCloseRate               AS annotation_close_rate
  , cardImpressions                   AS card_impressions
  , cardClicks                        AS card_clicks
  , cardClickRate                     AS card_click_rate
  , cardTeaserImpressions             AS card_teaser_impressions
  , cardTeaserClicks                  AS card_teaser_clicks
  , cardTeaserClickRate               AS card_teaser_click_rate
  , snippet_liveBroadcastContent      AS live_broadcast_content
  , snippet_localized_title           AS localized_title
  , snippet_localized_description     AS localized_description
  , contentDetails_dimension          AS content_details_dimension
  , contentDetails_definition         AS content_details_definition
  , contentDetails_caption            AS content_details_caption
  , contentDetails_licensedContent    AS content_details_licensed_content
  , contentDetails_projection         AS content_details_projection
  , contentDetails_hasCustomThumbnail AS content_details_has_custom_thumbnail
  , status_uploadStatus               AS status_upload_status
  , status_privacyStatus              AS status_privacy_status
  , status_license
  , status_embeddable
  , status_publicStatsViewable        AS status_public_stats_viewable
  , status_madeForKids                AS status_made_for_kids
  , status_selfDeclaredMadeForKids    AS status_self_declared_made_for_kids
  , __collected_ts
  , snapshot_date

FROM 
  {{ source('raw_social', 'youtube_contents') }}

  
