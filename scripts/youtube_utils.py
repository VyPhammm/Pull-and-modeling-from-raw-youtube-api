class Channels:
    SIPHER = "UCs8t-T2D2C2HIXt3VIKHz0A"

class Params_yt:
    DIMENSIONS = [
## View metrics
        "views",
        "redViews",
        # "viewerPercentage",
## Engagement metrics
        "likes",
        "dislikes",
        "shares",
        "comments",
        "subscribersGained",
        "subscribersLost",
        "videosAddedToPlaylists",
        "videosRemovedFromPlaylists",
## Watch time metrics
        "estimatedMinutesWatched",
        "estimatedRedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
## Annotations metrics
        "annotationImpressions",
        "annotationClickableImpressions",
        "annotationClicks",
        "annotationClickThroughRate",
        "annotationClosableImpressions",
        "annotationCloses",
        "annotationCloseRate",
## Card metrics
        "cardImpressions",
        "cardClicks",
        "cardClickRate",
        "cardTeaserImpressions",
        "cardTeaserClicks",
        "cardTeaserClickRate",
# ## Audience retention metrics
#         "audienceWatchRatio",
#         "relativeRetentionPerformance",
# ## Estimated revenue metrics
#         "estimatedRevenue",
#         "estimatedAdRevenue",
#         "estimatedRedPartnerRevenue",
# ## Ad performance metrics
#         "grossRevenue",
#         "cpm",
#         "adImpressions",
#         "monetizedPlaybacks",
#         "playbackBasedCpm",
]

    TRAFFIC_SOURCES = [
## View metrics
        "views",
## Watch time metrics
        "estimatedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
]

    CONTENTS = [
## View metrics
        "views",
        "redViews",
        # "viewerPercentage",
## Engagement metrics
        "likes",
        "dislikes",
        "shares",
        "comments",
        "subscribersGained",
        "subscribersLost",
        "videosAddedToPlaylists",
        "videosRemovedFromPlaylists",
## Watch time metrics
        "estimatedMinutesWatched",
        "estimatedRedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
## Annotations metrics
        "annotationImpressions",
        "annotationClickableImpressions",
        "annotationClicks",
        "annotationClickThroughRate",
        "annotationClosableImpressions",
        "annotationCloses",
        "annotationCloseRate",
## Card metrics
        "cardImpressions",
        "cardClicks",
        "cardClickRate",
        "cardTeaserImpressions",
        "cardTeaserClicks",
        "cardTeaserClickRate",
# ## Audience retention metrics
#         "audienceWatchRatio",
#         "relativeRetentionPerformance",
# ## Estimated revenue metrics
#         "estimatedRevenue",
#         "estimatedAdRevenue",
#         "estimatedRedPartnerRevenue",
# ## Ad performance metrics
#         "grossRevenue",
#         "cpm",
#         "adImpressions",
#         "monetizedPlaybacks",
#         "playbackBasedCpm",
]
    
    PART_CONTENT = [
        "contentDetails",
        "fileDetails",
        "id",
        "liveStreamingDetails",
        "localizations",
        "player",
        "processingDetails",
        "recordingDetails",
        "snippet",
        "statistics",
        "status",
        "suggestions",
        "topicDetails",
]
    REPORT_TYPES = {
        "overview":         {"partition_expr": "{channel_id:STRING}/{snapshot_date:DATE}"},  
        "traffic_sources":  {"partition_expr": "{channel_id:STRING}/{snapshot_date:DATE}"},
        "contents":         {"partition_expr": "{channel_id:STRING}/{snapshot_date:DATE}"},
        "geographic_areas": {"partition_expr": "{channel_id:STRING}/{snapshot_date:DATE}"}, 
        "demographics" :    {"partition_expr": "{channel_id:STRING}/{snapshot_date:DATE}"},
}   