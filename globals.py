from facebook_business.adobjects.adsinsights import AdsInsights

FACEBOOK_RATES = {"x-business-use-case-usage": None, "x-ad-account-usage": None, "x-app-usage": None}
DEFAULT_INSIGHTS_FIELDS = [
    AdsInsights.Field.ad_name,
    AdsInsights.Field.spend,
    AdsInsights.Field.actions,
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
ISSUES_FIELDS_PARAMS = {
    "fields": ["name", "id", "effective_status", "configured_status", "created_time"],
    "params": {
        "date_preset": "last_7d",
        "filtering": [
            {
                "field": "ad.effective_status",
                "operator": "IN",
                "value": ["DISAPPROVED", "WITH_ISSUES"],
            },
            # {"field": "ad.created_time", "operator": "GREATER_THAN", "value": 1675214578},
        ],
        "limit": 250,
    },
}
