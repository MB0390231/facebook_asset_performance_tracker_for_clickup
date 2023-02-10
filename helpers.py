from datetime import datetime, timedelta
from exceptions import ClickupObjectNotFound
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def datetime_to_epoch(datetime_string):
    """Converts a datetime string to an epoch timestamp.

    Arguments:
    datetime_string -- A string in the format "YYYY-MM-DD HH:MM:SS".

    Returns:
    An integer representing the number of seconds elapsed since 1970-01-01 00:00:00 (the epoch).
    """
    datetime_obj = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    return int(datetime_obj.timestamp() * 1000)


def generate_datetime_string(day_delta):
    target_date = datetime.now() + timedelta(days=day_delta)
    return target_date.strftime("%Y-%m-%d %H:%M:%S")


def get_clickup_list(client, team_name, space_name, list_name, folder_name=None):
    """
    Retrieve a specific list from Clickup dashboard.

    Args:
    - client (object): A client object that allows communication with the Clickup system.
    - team_name (str): The name of the team the space belongs to.
    - space_name (str): The name of the space the list belongs to.
    - list_name (str): The name of the list to retrieve.
    - folder_name (str, optional): The name of the folder that contains the list. If not provided, the function will search for the list in the space directly.

    Returns:
    - object: The requested list object.

    Raises:
    - ClickupObjectNotFound: If the team, space, folder, or list is not found.
    """
    try:
        found_team = [team for team in client.get_teams() if team["name"] == team_name][0]
    except IndexError:
        raise ClickupObjectNotFound(f"The Clickup team: {team_name} was not found.")
    try:
        found_spaces = [space for space in found_team.get_spaces() if space["name"] == space_name][0]
    except IndexError:
        raise ClickupObjectNotFound(f"The Clickup Space: {space_name} was not found.")
    if folder_name is not None:
        try:
            list_parent = [folder for folder in found_spaces.get_folders() if folder["name"] == folder_name][0]
        except IndexError:
            raise ClickupObjectNotFound(f"The Clickup Folder: {folder_name} was not found.")
    else:
        list_parent = found_spaces
    try:
        ret_list = [l for l in list_parent.get_lists() if l["name"] == list_name][0]
    except IndexError:
        raise ClickupObjectNotFound(f"The Clickup List: {list_name} was not found.")
    return ret_list


def get_ghl_appointments_by_location_and_calendar(ghl_client, clickup_task_list, start_day_delta, end_day_delta):
    """
    Retrieves appointment data from a given GHL client and task list.

    Parameters:
    - ghl_client (object): An object representing the GHL client.
    - clickup_task_list (list): A list of tasks, where each task is a dictionary containing information about the task.
    - start_day_delta (int): The number of days relative to the current date to use as the start date for retrieving appointments.
    - end_day_delta (int): The number of days relative to the current date to use as the end date for retrieving appointments.

    Returns:
    A dictionary with the following format:
    {
        location['name']:{
            calendar['name']:appointments
        }
    }

    The dictionary maps the names of locations to a dictionary of calendars, where the calendars are associated with their respective appointments.
    """
    appointment_params = {
        "startDate": datetime_to_epoch(generate_datetime_string(day_delta=start_day_delta)),
        "endDate": datetime_to_epoch(generate_datetime_string(day_delta=end_day_delta)),
    }
    locations = ghl_client.get_locations()
    appointment_data = {}
    for location in locations:
        for task in clickup_task_list:
            if location["id"] == get_custom_field_value(input_list=task["custom_fields"], name="Location ID"):
                calendars = location.get_calendar_services()
                appointment_data[location["name"]] = {}
                for calendar in calendars:
                    appointments = calendar.get_appointments(params=appointment_params)
                    appointment_data[location["name"]][calendar["name"]] = appointments
    return appointment_data


def custom_field_dict(custom_field_list):
    """
    Retrieves appointment data from a given GHL client and task list.

    Parameters:
    - ghl_client (object): An object representing the GHL client.
    - clickup_task_list (list): A list of tasks, where each task is a dictionary containing information about the task.
    - start_day_delta (int): The number of days relative to the current date to use as the start date for retrieving appointments.
    - end_day_delta (int): The number of days relative to the current date to use as the end date for retrieving appointments.

    Returns:
    A dictionary with the following format:
    {
        location['name']:{
            calendar['name']:appointments
        }
    }

    The dictionary maps the names of locations to a dictionary of calendars, where the calendars are associated with their respective appointments.
    """
    custom_field_dict = {}
    for field in custom_field_list:
        custom_field_dict[field["name"]] = field["id"]
    return custom_field_dict


def get_custom_field_value(input_list, name):
    """
    Retrieves the value for a given name from a list of objects.

    Parameters:
    - input_list (list): A list of objects, where each object is a dictionary containing information about the object.
    - name (str): The name that you want to retrieve the value for.

    Returns:
    The value associated with the given name, or None if no matching name is found in the list.
    """
    for item in input_list:
        if item["name"] == name:
            return item.get("value")
    return None


def facebook_data_organized_by_date_preset(business, date_presets):
    """
    Retrieves data at for a variety of date presets. Returns a dictionary of ad data with the following format. It is organized by date presets

    return format = {
       "date_preset":{

    }
    """
    ad_accounts = facebook_business_active_ad_accounts(business=business)
    data = {}
    for presets in date_presets:
        data[presets] = facebook_ad_accounts_ad_data(ad_accounts, presets)
    return data


def facebook_ad_accounts_ad_data(ad_accounts, date_preset=None):
    """
    Given a list of ad_accounts, this function retreives ad data for preset date and returns a dictionary with the following format
    {
        "retailer id":{
                "spend":spend,
                "actions":actions,
            },
            "unsucessful_reports":[list of strings representing account ids],
            "unsucessful_requests":[list of future objects]
    }
    """
    # generate list of ad reports using threads
    succesfully_created_reports, unsuccessful_requests = process_ad_account_requests(
        ad_accounts, date_preset=date_preset
    )
    # TODO: Retry with the unsucessful reports
    ads, unsucessful_reports = process_ad_reports(succesfully_created_reports)
    ads = facebook_ad_data_organized_by_retailer_id(ads)

    return ads, unsucessful_reports, unsuccessful_requests


def facebook_business_active_ad_accounts(business):
    """
    Returns a list of active Facebook ad accounts associated with a given business.

    Parameters:
    - business (object): An object representing the Facebook business.

    Returns:
    - active_ad_accounts (list): A list of dictionaries representing active Facebook ad accounts, with each dictionary containing the following keys:
        - "account_status": the status of the account (1 for active)
        - "name": the name of the account
        - "id": the ID of the account

    The returned list will only include unique active accounts, regardless of whether they are client-owned or business-owned.
    """

    active_ad_accounts = []
    alread_added = []
    client_owned = business.get_client_ad_accounts(fields=["account_status", "name", "id"])
    business_owned = business.get_owned_ad_accounts(fields=["account_status", "name", "id"])
    for accounts in [*client_owned, *business_owned]:
        if accounts["account_status"] == 1 and accounts["id"] not in alread_added:
            active_ad_accounts.append(accounts)
    return active_ad_accounts


def process_ad_account_requests(active_ad_accounts, date_preset):
    """
    This function returns a list of facebook.adobject.adreportrun objects for each ad account given.

    Parameters:
        - active_ad_accounts (list): A list of objects representing the active Facebook Ad accounts.
        - date_preset (str): The date preset to use while retrieving insights data.

    Returns:
    A tuple with two elements:
        - ad_reports (list): A list of reports, where each report is a facebook.adobject.adreportrun object.
        - unsuccessful_requests (list): A list of unsuccessful request futures.

    """

    ad_reports = []
    unsuccessful_requests = []
    fields = [
        AdsInsights.Field.ad_name,
        AdsInsights.Field.spend,
        AdsInsights.Field.actions,
        AdsInsights.Field.ad_id,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.adset_id,
        AdsInsights.Field.account_id,
        AdsInsights.Field.created_time,
    ]
    params = {
        "level": "ad",
        "date_preset": date_preset,
        "filtering": [
            {
                "field": "ad.effective_status",
                "operator": "IN",
                "value": ["ACTIVE", "PAUSED", "ADSET_PAUSED", "CAMPAIGN_PAUSED"],
            },
        ],
    }
    with ThreadPoolExecutor() as executor:
        jobs_queue = [executor.submit(create_async_job, account, fields, params) for account in active_ad_accounts]
        for future in as_completed(jobs_queue):
            try:
                result = future.result()
                ad_reports.append(result)
            except:
                unsuccessful_requests.append(future)
    return ad_reports, unsuccessful_requests


def process_ad_reports(ad_reports):
    """
    Processes Facebook Ad reports.

    Parameters:
        - ad_reports (list): A list of Facebook AdReportRun objects.

    Returns:
        A tuple two elemetns
            - ads (list): A list of ads, where each ad is a Facebook Ad object.
            - unsuccessful_reports (list): A list of unsuccessful reports.

    The function retrieves the results of the Facebook AdReportRun objects in `ad_reports` and adds the successful results to a list of Facebook Ad objects. If a report run is unsuccessful, it is added to a list of unsuccessful report runs. The function returns a tuple of the list of processed Facebook Ad objects and the list of unsuccessful report runs.
    """
    ads = []
    unsuccessful_reports = []
    with ThreadPoolExecutor() as executor:
        results_queue = [executor.submit(wait_for_job, report) for report in ad_reports]
        for future in as_completed(results_queue):
            result = future.result()
            if result[AdReportRun.Field.async_status].lower() == "job completed":
                cursor = result.get_result(params={"limit": 1000})
                ads.extend(cursor)
            else:
                unsuccessful_reports.append(result)
    return ads, unsuccessful_reports


def create_async_job(account, fields, params):
    """
    The function creates a facebook.adobject.adreportrun object for a given ad account.

    Parameters:
        - account (object): An object representing the Facebook Ad account.
        - fields (list): A list of fields to be retrieved from the account insights data.
        - params (dict): A dictionary of parameters to be used while retrieving insights data.

    Returns:
        An facebook.adobject.adreportrun object.
    """
    job = account.get_insights_async(fields=fields, params=params)
    print(f"Created report for account: {account['id']}")
    return job.api_get()


def wait_for_job(job):
    """
    Waits for the completion of a job submitted to the Facebook Ad Insights API.

    Parameters:
        - job (object): A facebook.adobject.adreportrun object representing the submitted job.

    Returns:
       A facebook.adobject.adreportfun object.

    The function waits for the completion of the job submitted to the Facebook Ad Insights API.
    """
    job = job.api_get()
    account_id = job["account_id"]
    print(f"Waiting for account: {account_id}")
    while (
        job[AdReportRun.Field.async_percent_completion] < 100 or job[AdReportRun.Field.async_status] == "Job Running"
    ):
        if job[AdReportRun.Field.async_status] == "Job Failed":
            return job
        time.sleep(10)
        job = job.api_get()
    string = f"ID:{account_id} Type:result"
    print(string)
    return job.api_get()
