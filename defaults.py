"""
I plan on adding a feature to allow users to specify their own defaults
"""

from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.ad import Ad
from helpers.helpers import week_ago_epoch_timestamp

DEFAULT_INSIGHTS_FIELDS = [
    AdsInsights.Field.ad_name,
    AdsInsights.Field.spend,
    AdsInsights.Field.actions,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cost_per_inline_link_click,
    AdsInsights.Field.inline_link_click_ctr,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.adset_id,
    AdsInsights.Field.account_id,
    AdsInsights.Field.created_time,
]
DEFAULT_INSIGHTS_PARAMS = {
    "level": "ad",
    "filtering": [
        {
            "field": "ad.effective_status",
            "operator": "IN",
            "value": ["ACTIVE", "PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED", "DISAPPROVED"],
        },
        {
            "field": "ad.spend",
            "operator": "GREATER_THAN",
            "value": 0,
        },
    ],
    "limit": 250,
}

DEFAULT_ISSUES_FIELDS = [
    Ad.Field.name,
    Ad.Field.id,
    Ad.Field.configured_status,
    Ad.Field.created_time,
    Ad.Field.updated_time,
]


DEFAULT_ISSUES_PARAMS = {
    "filtering": [
        {
            "field": "ad.effective_status",
            "operator": "IN",
            "value": ["DISAPPROVED", "WITH_ISSUES"],
        },
        {"field": "ad.updated_time", "operator": "GREATER_THAN", "value": week_ago_epoch_timestamp()},
        {"field": "ad.created_time", "operator": "GREATER_THAN", "value": week_ago_epoch_timestamp()},
    ],
    "limit": 200,
}


DEFAULT_DATE_PRESETS = ["maximum", "last_30d", "last_7d", "last_3d"]
