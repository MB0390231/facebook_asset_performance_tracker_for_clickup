"""
I plan on adding a feature to allow users to specify their own defaults
"""

from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.ad import Ad

DEFAULT_INSIGHTS_FIELDS = [
    AdsInsights.Field.ad_name,
    AdsInsights.Field.spend,
    AdsInsights.Field.actions,
    AdsInsights.Field.cost_per_inline_link_click,
    AdsInsights.Field.cpm,
    AdsInsights.Field.cpc,
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
            "value": ["ACTIVE", "PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED"],
        },
    ],
}

DEFAULT_ISSUES_FIELDS = [
    Ad.Field.name,
    Ad.Field.id,
    Ad.Field.effective_status,
    Ad.Field.configured_status,
    Ad.Field.created_time,
    Ad.Field.ad_review_feedback,
    Ad.Field.failed_delivery_checks,
    Ad.Field.issues_info,
    Ad.Field.updated_time,
]


DEFAULT_ISSUES_PARAMS = {
    "filtering": [
        {
            "field": "ad.effective_status",
            "operator": "IN",
            "value": ["DISAPPROVED", "WITH_ISSUES"],
        },
    ],
    "limit": 200,
}


DEFAULT_DATE_PRESETS = ["maximum", "last_30d"]
