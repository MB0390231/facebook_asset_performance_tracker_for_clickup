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
FB.generate_data_for_date_presets(FB.active_accounts, DATE_PRESETS, DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS)
FB.generate_data_for_batch_date_presets(
    FB.accounts["ACTIVE"], DATE_PRESETS, DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS
)
