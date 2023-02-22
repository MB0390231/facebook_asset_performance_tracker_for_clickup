from gohighlevel_python_sdk.ghlobjects.objects import Agency
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.business import Business
from clickup_python_sdk.api import ClickupClient
import helpers
import os


def main():
    CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")
    FB_TOKEN = os.environ.get("FB_TOKEN")
    APP_ID = os.environ.get("FACEBOOK_APP_ID")
    BUSINESS_ID = os.environ.get("BUSINESS_ID")
    GHL_KEY = os.environ.get("GHL_TOKEN")
    RGM_CLICKUP = ClickupClient.init(user_token=CLICKUP_TOKEN)
    LOGS = helpers.get_clickup_list(
        client=RGM_CLICKUP,
        team_name="ReDefined Growth Marketing",
        space_name="North Star",
        folder_name="rgm-northstar-clickup",
        list_name="Logs",
    ).get_tasks()

    NORTHSTAR = helpers.get_clickup_list(
        client=RGM_CLICKUP,
        team_name="ReDefined Growth Marketing",
        space_name="North Star",
        folder_name="rgm-northstar-clickup",
        list_name="Northstar",
    )
    NS_TASKS = NORTHSTAR.get_tasks()
    NS_FIELDS = helpers.custom_field_dict(NORTHSTAR.get_custom_fields())

    RGM = Agency(agency_token=GHL_KEY)
    APPOINTMENTS = helpers.get_ghl_appointments_by_location_and_calendar(
        RGM, clickup_task_list=NS_TASKS, start_day_delta=-30, end_day_delta=120
    )

    FACEBOOK_CLIENT = FacebookAdsApi.init(app_id=APP_ID, access_token=FB_TOKEN)
    FACEBOOK_DATA = {
        "accounts": helpers.facebook_business_active_ad_accounts(Business(BUSINESS_ID)),
    }
    FACEBOOK_DATA["ads_with_issues"] = helpers.ads_with_issues(FACEBOOK_DATA["accounts"])
    FACEBOOK_DATA["data"] = helpers.facebook_data_organized_by_date_preset(
        FACEBOOK_DATA["accounts"], ["last_7d", "last_30d", "last_3d"]
    )
    RETAILER_DATA = {
        "last_30d": helpers.consolidate_ad_stats(FACEBOOK_DATA["data"]["last_30d"]["ads_data"], "ad_name", r"^\d{3}"),
        "last_7d": helpers.consolidate_ad_stats(FACEBOOK_DATA["data"]["last_7d"]["ads_data"], "ad_name", r"^\d{3}"),
        "last_3d": helpers.consolidate_ad_stats(FACEBOOK_DATA["data"]["last_3d"]["ads_data"], "ad_name", r"^\d{3}"),
        "ads_with_issues": helpers.facebook_data_organized_by_regex(
            FACEBOOK_DATA["ads_with_issues"]["data"], "name", r"^\d{3}"
        ),
    }

    CLICKUP_JOBS = helpers.create_clickup_jobs(NS_TASKS, NS_FIELDS, APPOINTMENTS, RETAILER_DATA)
    CLICKUP_STATUS = helpers.process_clickup_jobs(RGM_CLICKUP, CLICKUP_JOBS)

    # upload data files to clickup
    # helpers.write_all_data_to_csv(FACEBOOK_DATA, APPOINTMENTS)
    # helpers.upload_to_clickup(LOGS)

    helpers.log_report_errors(FACEBOOK_DATA)
    # run through all exceptions and print them


if __name__ == "__main__":
    main()
