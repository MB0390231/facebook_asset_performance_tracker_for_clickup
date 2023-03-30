from helpers.datacontainers import FaceBookDataContainer
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adsinsights import AdsInsights
from helpers.reportbuilder import ReportBuilder, consolidate_facebook_reporting_data
from helpers.clickupjobpool import ClickupJobPool
from clickup_python_sdk.api import ClickupClient
from clickup_python_sdk.clickupobjects.folder import Folder
from helpers.helpers import current_date, convert_timestamp_to_date, upload_to_clickup
from defaults import DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS, DEFAULT_DATE_PRESETS
import os

APP_ID = os.environ.get("APP_ID")
BUSINESS_ID = os.environ.get("BUSINESS_ID")
FB_TOKEN = os.environ.get("FB_TOKEN")
CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
ASSET_TRACKER_FOLDER_ID = os.environ.get("ASSET_TRACKER_FOLDER_ID")


INSIGHTS_REPORT = {
    "spend": 0,
    "count": 0,
    "actions": {},
    "cpm": 0,
    "cost_per_inline_link_click": 0,
    "inline_link_click_ctr": 0,
}

CLICKUP = ClickupClient.init(user_token=CLICKUP_TOKEN)
ASSETS_TRACKER_LISTS = Folder(id=ASSET_TRACKER_FOLDER_ID).get_lists()
INFORMATION = [l for l in ASSETS_TRACKER_LISTS if l["name"].lower() == "information"][0]
CLICKUP_INFORMATION_TASKS = INFORMATION.get_tasks()

# gather the task names to use as assets and build a report
ASSETS_LIST = [l for l in ASSETS_TRACKER_LISTS if l["name"].lower() == "assets"][0]
CLICKUP_ASSET_TASKS = ASSETS_LIST.get_tasks()
CLICKUP_ASSET_FIELDS = {field["name"]: field["id"] for field in ASSETS_LIST.get_custom_fields()}
TASKS_NAMES = [task["name"] for task in CLICKUP_ASSET_TASKS]

CAMPAIGN_DATA = [l for l in ASSETS_TRACKER_LISTS if l["name"] == "Ads Data (copy)"][0]
CAMPAIGN_ASSET_TASKS = CAMPAIGN_DATA.get_tasks()
CAMPAIGN_ASSET_FIELDS = {field["name"]: field["id"] for field in CAMPAIGN_DATA.get_custom_fields()}

FB_CLIENT = FacebookAdsApi.init(app_id=APP_ID, access_token=FB_TOKEN)
POOL = ClickupJobPool(clickup_client=CLICKUP)


FB_CAMPAIGNS = FaceBookDataContainer().set_business_ad_accounts(business_id=BUSINESS_ID)
for dates in DEFAULT_DATE_PRESETS:
    fields = [
        AdsInsights.Field.ad_name,
        AdsInsights.Field.spend,
        AdsInsights.Field.actions,
        AdsInsights.Field.cpm,
        AdsInsights.Field.cost_per_inline_link_click,
        AdsInsights.Field.inline_link_click_ctr,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.account_id,
        AdsInsights.Field.created_time,
    ]
    params = {
        "level": "campaign",
        "filtering": [
            {
                "field": "campaign.effective_status",
                "operator": "IN",
                "value": ["ACTIVE", "PAUSED", "CAMPAIGN_PAUSED", "WITH_ISSUES"],
            },
            {
                "field": "campaign.spend",
                "operator": "GREATER_THAN",
                "value": 0,
            },
        ],
        "date_preset": dates,
    }

    params = DEFAULT_INSIGHTS_PARAMS.copy()
    params["date_preset"] = dates
    params["level"] = "campaign"
    FB_CAMPAIGNS.retrieve_facebook_objects_insights(FB_CAMPAIGNS.accounts["ACTIVE"], fields=fields, params=params)

CAMPAIGN_REPORT = {}
for dates, reporting_data in FB_CAMPAIGNS.reporting_data.items():
    report = ReportBuilder(default_report=INSIGHTS_REPORT)
    for tasks in CAMPAIGN_ASSET_TASKS:
        for campaign in reporting_data:
            target_field = [
                field.get("value", None) for field in tasks["custom_fields"] if field["name"].lower() == "id"
            ]
            if not target_field:
                continue
            if target_field[0] not in campaign["campaign_name"]:
                continue
            report.add_asset(asset_name=target_field[0], asset=campaign, method=consolidate_facebook_reporting_data)
    CAMPAIGN_REPORT[dates] = report

for tasks in CAMPAIGN_ASSET_TASKS:
    POOL.create_insights_jobs(
        tasks,
        CAMPAIGN_ASSET_FIELDS,
        asset_data=CAMPAIGN_REPORT,
        asset_match_type="custom_field",
        custom_field_location="id",
    )

# init the facebook data container
FB_ADS = FaceBookDataContainer().set_business_ad_accounts(business_id=BUSINESS_ID)
for dates in DEFAULT_DATE_PRESETS:
    fields = DEFAULT_INSIGHTS_FIELDS.copy()
    params = DEFAULT_INSIGHTS_PARAMS.copy()
    params["date_preset"] = dates
    FB_ADS.retrieve_facebook_objects_insights(FB_ADS.accounts["ACTIVE"], fields=fields, params=params)


ASSETS_REPORT = {}

# build report
for dates, reporting_data in FB_ADS.reporting_data.items():
    report = ReportBuilder(default_report=INSIGHTS_REPORT)
    for ads in reporting_data:
        for asset_name in TASKS_NAMES:
            if asset_name not in ads["ad_name"]:
                continue
            report.add_asset(asset_name=asset_name.lower(), asset=ads, method=consolidate_facebook_reporting_data)
    ASSETS_REPORT[dates] = report


for tasks in CLICKUP_ASSET_TASKS:
    POOL.create_insights_jobs(tasks, CLICKUP_ASSET_FIELDS, asset_data=ASSETS_REPORT)

POOL.process_clickup_jobs()

upload_to_clickup(tasks=CLICKUP_INFORMATION_TASKS)
TOKEN_DATA = FB_CLIENT.call("GET", f"https://graph.facebook.com/v16.0/debug_token?input_token={FB_TOKEN}").json()
update_info = values = {
    "content": f"Last Updated: {current_date()}\nFacebook Token Expiration: {convert_timestamp_to_date(TOKEN_DATA['data']['expires_at'])}"
}

for lists in [ASSETS_LIST, CAMPAIGN_DATA]:
    update = lists.update(update_info)
