from facebook_business.api import FacebookAdsApi
from helpers.facebookdatacontainer import FacebookDataContainer
from helpers.facebookassetgroup import FacebookAssetGroups
from clickup_python_sdk.api import ClickupClient
from helpers.clickupjobpool import ClickupJobPool
from keys import fb_token, app_id, business_id, clickup_token
from helpers import helpers
from defaults import (
    DEFAULT_INSIGHTS_FIELDS,
    DEFAULT_INSIGHTS_PARAMS,
    RGM_CLICKUP_TEAM,
    RGM_ASSET_TRACKERS_SPACE,
    ASSET_TRACKER,
    DATE_PRESETS,
)


CU = ClickupClient.init(clickup_token)
FB = FacebookDataContainer(
    FacebookAdsApi.init(app_id=app_id, access_token=fb_token),
)
FB.set_business_ad_accounts(business_id)
FB.generate_data_for_batch_date_presets(
    FB.accounts["ACTIVE"], DATE_PRESETS, DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS
)

ASSETS = FacebookAssetGroups(FB)
COPIES = ASSETS.add_asset_group("ad_name", r"ADC#[a-zA-Z0-9]+", "COPIES")
CREATIVES = ASSETS.add_asset_group("ad_name", r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+", "CREATIVES")


COPY_TRACKER = helpers.get_clickup_list(
    client=CU,
    team_name=RGM_CLICKUP_TEAM,
    space_name=RGM_ASSET_TRACKERS_SPACE,
    folder_name=ASSET_TRACKER,
    list_name="Copies",
)
CREATIVE_TRACKER = helpers.get_clickup_list(
    client=CU,
    team_name=RGM_CLICKUP_TEAM,
    space_name=RGM_ASSET_TRACKERS_SPACE,
    folder_name=ASSET_TRACKER,
    list_name="Creatives",
)
LOGS = helpers.get_clickup_list(
    client=CU,
    team_name=RGM_CLICKUP_TEAM,
    space_name=RGM_ASSET_TRACKERS_SPACE,
    folder_name=ASSET_TRACKER,
    list_name="Logs",
).get_tasks()

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
    helpers.write_ads_to_csv(FB.ads_data[dates], f"uploads/{dates}.csv")
helpers.upload_to_clickup(LOGS)
