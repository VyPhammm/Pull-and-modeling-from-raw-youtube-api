{{- config(
  materialized='view'
) -}}

SELECT
    channel
    , channel_id
    , country
    , views
    , redViews
    , likes
    , dislikes
    , shares
    , comments
    , subscribersGained                 AS subscribers_gained
    , subscribersLost                   AS subscribers_lost
    , videosAddedToPlaylists            AS videos_added_to_playlists
    , videosRemovedFromPlaylists        AS videos_removed_from_playlists
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
    , __collected_ts
    , snapshot_date
FROM 
  {{ source('raw_social', 'youtube_geographic_areas') }}