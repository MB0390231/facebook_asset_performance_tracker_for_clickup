from facebook_business.api import FacebookAdsApi
from helpers.facebookdatacontainer import FacebookDataContainer
from helpers.facebookassetgroup import FacebookAssetGroups
from clickup_python_sdk.api import ClickupClient
from helpers.clickupjobpool import ClickupJobPool
from clickup_python_sdk.clickupobjects.folder import Folder
from helpers import helpers
import os
from defaults import (
    DEFAULT_INSIGHTS_FIELDS,
    DEFAULT_INSIGHTS_PARAMS,
    ASSET_TRACKER_FOLDER_ID,
    DATE_PRESETS,
)

APP_ID = os.environ["APP_ID"]
FB_TOKEN = os.environ["FB_TOKEN"]
BUSINESS_ID = os.environ["BUSINESS_ID"]
CLICKUP_TOKEN = os.environ["CLICKUP_TOKEN"]


FB = FacebookDataContainer(FacebookAdsApi.init(APP_ID=APP_ID, access_token=FB_TOKEN))

FB.set_business_ad_accounts(BUSINESS_ID)
FB.generate_data_for_batch_date_presets(
    FB.accounts["ACTIVE"], DATE_PRESETS, DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS
)

ASSETS = FacebookAssetGroups(FB)
COPIES = ASSETS.add_asset_group("ad_name", r"ADC#[a-zA-Z0-9]+", "COPIES")
CREATIVES = ASSETS.add_asset_group("ad_name", r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+", "CREATIVES")


CU = ClickupClient.init(CLICKUP_TOKEN)
ASSET_TRACKER_FOLDER = Folder(id=ASSET_TRACKER_FOLDER_ID)

CLICKUP_ASSETS = ASSET_TRACKER_FOLDER.get_lists()
COPY_TRACKER = [list for list in CLICKUP_ASSETS if list["name"] == "Copies"][0]
CREATIVE_TRACKER = [list for list in CLICKUP_ASSETS if list["name"] == "Creatives"][0]
LOGS = [list for list in CLICKUP_ASSETS if list["name"] == "Logs"][0]

COPY_TRACKER_FIELDS = {field["name"]: field["id"] for field in COPY_TRACKER.get_custom_fields()}
CREATIVE_TRACKER_FIELDS = {field["name"]: field["id"] for field in CREATIVE_TRACKER.get_custom_fields()}
COPY_TRACKER_TASKS = COPY_TRACKER.get_tasks()
CREATIVE_TRACKER_TASKS = CREATIVE_TRACKER.get_tasks()
POOL = ClickupJobPool(clickup_client=CU)


for task in COPY_TRACKER_TASKS:
    POOL.create_insights_jobs(task, COPY_TRACKER_FIELDS, ASSETS.reports["COPIES"])
for task in CREATIVE_TRACKER_TASKS:
    POOL.create_insights_jobs(task, CREATIVE_TRACKER_FIELDS, ASSETS.reports["CREATIVES"])

POOL.process_clickup_jobs()

for dates in FB.date_presets:
    helpers.write_ads_to_csv(FB.insights_data[dates], f"uploads/{dates}.csv")
helpers.upload_to_clickup(LOGS.get_tasks())
