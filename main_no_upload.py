from helpers.datacontainers import FaceBookDataContainer
from facebook_business.api import FacebookAdsApi
from helpers.clickupjobpool import ClickupJobPool
from clickup_python_sdk.api import ClickupClient
from clickup_python_sdk.clickupobjects.folder import Folder
from helpers.reportbuilder import consolidate_facebook_reporting_data, ReportBuilder, add_ads_with_issues
from helpers.helpers import extract_assets, upload_to_clickup
import os
from defaults import (
    DEFAULT_INSIGHTS_FIELDS,
    DEFAULT_DATE_PRESETS,
    DEFAULT_INSIGHTS_PARAMS,
    DEFAULT_ISSUES_FIELDS,
    DEFAULT_ISSUES_PARAMS,
)

FB_TOKEN = os.environ.get("FB_TOKEN")
APP_ID = os.environ.get("APP_ID")
BUSINESS_ID = os.environ.get("BUSINESS_ID")
CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
ASSET_TRACKER_FOLDER_ID = os.environ.get("ASSET_TRACKER_FOLDER_ID")
NORTHSTAR_FOLDER_ID = os.environ.get("NORTHSTAR_FOLDER_ID")


CLICKUP = ClickupClient.init(user_token=CLICKUP_TOKEN)
ASSETS = Folder(ASSET_TRACKER_FOLDER_ID)
ASSETS_LISTS = ASSETS.get_lists()
INFORMATION = [l for l in ASSETS_LISTS if l["name"] == "Information"][0]
CREATIVE_TRACKER_LIST = [l for l in ASSETS_LISTS if l["name"] == "Creative Data Tracker"][0]
CREATIVE_TRACKER_TASKS = CREATIVE_TRACKER_LIST.get_tasks()
CREATIVE_TRACKER_FIELDS = {field["name"]: field["id"] for field in CREATIVE_TRACKER_LIST.get_custom_fields()}
COPY_TRACKER_LIST = [l for l in ASSETS_LISTS if l["name"] == "Copy Data Tracker"][0]
COPY_TRACKER_TASKS = COPY_TRACKER_LIST.get_tasks()
COPY_TRACKER_FIELDS = {field["name"]: field["id"] for field in COPY_TRACKER_LIST.get_custom_fields()}
NORTHSTAR_FOLDER = Folder(NORTHSTAR_FOLDER_ID).get()
NORTH_LISTS = NORTHSTAR_FOLDER.get_lists()
NORTHSTAR = [l for l in NORTH_LISTS if l["name"] == "Northstar"][0]
NORTHSTAR_TASKS = NORTHSTAR.get_tasks()
NORTHSTAR_FIELDS = {field["name"]: field["id"] for field in NORTHSTAR.get_custom_fields()}


FB_CLIENT = FacebookAdsApi.init(access_token=FB_TOKEN, app_id=APP_ID, app_secret=None)
FB = FaceBookDataContainer().set_business_ad_accounts(BUSINESS_ID)
# for date_presets in DEFAULT_DATE_PRESETS:
for date_presets in DEFAULT_DATE_PRESETS:
    params = DEFAULT_INSIGHTS_PARAMS.copy()
    params["date_preset"] = date_presets
    FB.retrieve_facebook_objects_insights(FB.accounts["ACTIVE"], DEFAULT_INSIGHTS_FIELDS, params)
FB.retreve_ads(FB.accounts["ACTIVE"], DEFAULT_ISSUES_FIELDS, DEFAULT_ISSUES_PARAMS)
ASSET_MAPPING = {
    "retailer_id": r"^\d{3}",
    "creative_id": r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+",
    "copy_id": r"ADC#[a-zA-Z0-9]+",
}
INSIGHTS_REPORT = {
    "spend": 0,
    "count": 0,
    "actions": {},
    "cpm": 0,
    "cost_per_inline_link_click": 0,
    "inline_link_click_ctr": 0,
}
ISSUES_REPORT = ReportBuilder(default_report={"issues": 0})
ASSETS_DATA = {}


for dates, values in FB.reporting_data.items():
    report = ReportBuilder(default_report=INSIGHTS_REPORT)
    for obj in values:
        assets = extract_assets(obj["ad_name"], ASSET_MAPPING)

        for value in assets.values():
            if value == "":
                continue
            report.add_asset(asset_name=value.lower(), asset=obj, method=consolidate_facebook_reporting_data)
    ASSETS_DATA[dates] = report
for ads in FB.ads:
    assets = extract_assets(ads["name"], ASSET_MAPPING)
    for value in assets.values():
        if value == "":
            continue
        ISSUES_REPORT.add_asset(asset_name=value.lower(), asset=ads, method=add_ads_with_issues)

# POOL = ClickupJobPool(clickup_client=CLICKUP)
POOL = ClickupJobPool(clickup_client=None)
for task in NORTHSTAR_TASKS:
    POOL.create_insights_jobs(
        task, NORTHSTAR_FIELDS, ASSETS_DATA, asset_match_type="custom_field", custom_field_location="ID"
    )
    POOL.create_ads_with_issues_jobs(
        task, NORTHSTAR_FIELDS, ISSUES_REPORT, asset_match_type="custom_field", custom_field_location="ID"
    )
for task in CREATIVE_TRACKER_TASKS:
    POOL.create_insights_jobs(task, CREATIVE_TRACKER_FIELDS, ASSETS_DATA)
    POOL.create_ads_with_issues_jobs(task, CREATIVE_TRACKER_FIELDS, ISSUES_REPORT)

for task in COPY_TRACKER_TASKS:
    POOL.create_insights_jobs(task, COPY_TRACKER_FIELDS, ASSETS_DATA)
    POOL.create_ads_with_issues_jobs(task, COPY_TRACKER_FIELDS, ISSUES_REPORT)

# upload runtime_logs
upload_to_clickup(INFORMATION.get_tasks)
