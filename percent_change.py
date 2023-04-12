from helpers.datacontainers import FaceBookDataContainer
from helpers.reportbuilder import ReportBuilder, consolidate_facebook_reporting_data, insights_percent_difference
from facebook_business.api import FacebookAdsApi
from clickup_python_sdk.api import ClickupClient
from clickup_python_sdk.clickupobjects.folder import Folder
from helpers.clickupjobpool import ClickupJobPool
from helpers.helpers import match_time_window
from defaults import DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS
import os

APP_ID = os.environ.get("APP_ID")
BUSINESS_ID = os.environ.get("BUSINESS_ID")
FB_TOKEN = os.environ.get("FB_TOKEN")
CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
ASSET_TRACKER_FOLDER_ID = os.environ.get("ASSET_TRACKER_FOLDER_ID")


CLICKUP = ClickupClient.init(user_token=CLICKUP_TOKEN)
ASSETS_TRACKER_LISTS = Folder(id=ASSET_TRACKER_FOLDER_ID).get_lists()

# gather the task names to use as assets and build a report
ASSETS_LIST = [l for l in ASSETS_TRACKER_LISTS if l["name"].lower() == "assets"][0]
CLICKUP_ASSET_TASKS = ASSETS_LIST.get_tasks()
CLICKUP_ASSET_FIELDS = {field["name"]: field["id"] for field in ASSETS_LIST.get_custom_fields()}
ASSETS = [task["name"].rstrip() for task in CLICKUP_ASSET_TASKS]

POOL = ClickupJobPool(clickup_client=CLICKUP)

params = DEFAULT_INSIGHTS_PARAMS.copy()
fields = DEFAULT_INSIGHTS_FIELDS.copy()
params["time_increment"] = "1"
params["date_preset"] = "last_3d"

FB = FacebookAdsApi.init(access_token=FB_TOKEN, app_id=APP_ID)

fb_data = FaceBookDataContainer().set_business_ad_accounts(business_id=BUSINESS_ID)
fb_data.retrieve_facebook_objects_insights(fb_data.accounts["ACTIVE"], fields, params)

default_report = {
    "spend": 0,
    "count": 0,
    "actions": {},
    "cpm": 0,
    "cost_per_inline_link_click": 0,
    "inline_link_click_ctr": 0,
}
two_days_ago_report = ReportBuilder(default_report)
yesterday_report = ReportBuilder(default_report)

for ads in fb_data.reporting_data["last_3d"]:
    report = None
    if match_time_window(ads["date_start"], ads["date_stop"], 1, 1):
        report = yesterday_report
    elif match_time_window(ads["date_start"], ads["date_stop"], 2, 2):
        report = two_days_ago_report
    for asset in ASSETS:
        if asset not in ads["ad_name"] or report is None:
            continue
        report.add_asset(asset, ads, consolidate_facebook_reporting_data)

percent_report = {}
for asset in ASSETS:
    prev_asset = two_days_ago_report[asset]
    current_asset = yesterday_report[asset]
    percent_report[asset] = insights_percent_difference(prev_asset, current_asset)

for tasks in CLICKUP_ASSET_TASKS:
    POOL.create_insights_percent_job(tasks, CLICKUP_ASSET_FIELDS, percent_report[tasks["name"]])

POOL.process_clickup_jobs()
