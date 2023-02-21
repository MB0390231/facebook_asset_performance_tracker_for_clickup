from datetime import datetime, timedelta
from exceptions import ClickupObjectNotFound
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
import time, json, re, globals, copy, csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import sys
import math
import csv


def datetime_to_epoch(datetime_string):
    """Converts a datetime string to an epoch timestamp.

    Arguments:
        - datetime_string -- A string in the format "YYYY-MM-DD HH:MM:SS".

    Returns:
        - An integer representing the number of seconds elapsed since 1970-01-01 00:00:00 (the epoch).
    """
    datetime_obj = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    return int(datetime_obj.timestamp() * 1000)


def generate_datetime_string(day_delta):
    """
    Generate a datetime string representing a target date.

    Parameters:
        - day_delta (int): The number of days before or after today to generate the target date.

    Returns:
        - datetime_string (str): A string representation of the target date in the format "YYYY-MM-DD HH:MM:SS"."""
    target_date = datetime.now() + timedelta(days=day_delta)
    if day_delta >= 0:
        return target_date.strftime("%Y-%m-%d 23:59:59")
    else:
        return target_date.strftime("%Y-%m-%d 00:00:00")


def count_appointments(appointments):
    """
    This function counts the number of appointments that were created in the past 7 days and the appointments that are in the future.
    The appointments that were created in the past 7 days (not including today) are stored in a list with key "APPT 7".
    The appointments that are in the future are stored in a list with key "APPT FUT".

    Args:
    appointments: list of dictionaries. Each dictionary represents a single appointment and contains the following keys:
        "appoinmentStatus": string value indicating the status of the appointment.
        "startTime": string value indicating the start time of the appointment in the format "%Y-%m-%d".
        "createdAt": string value indicating the creation time of the appointment in the format "%Y-%m-%d".

    Returns:
    dictionary: containing two keys: "APPT 7" and "APPT FUT". Each key contains a list of appointments that meet the criteria described above.
    """
    current_time = datetime.now().date()
    last_7d_appt = []
    future_appt = []
    seven_days_ago = current_time - timedelta(days=8)

    for appointment in appointments:
        if appointment["appoinmentStatus"] == "confirmed":
            start_time = datetime.strptime(appointment["startTime"][:10], "%Y-%m-%d").date()
            created_time = datetime.strptime(appointment["createdAt"][:10], "%Y-%m-%d").date()
            if created_time >= seven_days_ago and created_time < current_time:
                last_7d_appt.append(appointment)
            if start_time > current_time:
                future_appt.append(appointment)
    ret = {
        "APPT 7": last_7d_appt,
        "APPT FUT": future_appt,
    }
    return ret


def create_clickup_jobs(tasks, custom_fields, ghl_appointments, facebook_data):
    appointment_jobs = create_ghl_clickup_field_jobs(tasks, custom_fields, ghl_appointments)
    facebook_jobs = create_clickup_facebook_jobs(tasks, custom_fields, facebook_data)
    return [*appointment_jobs, *facebook_jobs]


def create_ghl_clickup_field_jobs(tasks, custom_fields, appts):
    """
    Args:
        - tasks (list): A list of dictionaries, where each dictionary represents a single task.
        - custom_fields (dict): A dictionary containing the custom fields information.
        - appts (dict): A dictionary where the keys are location ids and the values are lists of appointments.

    Returns:
        - list: A list of tuples, where each tuple contains the task, custom field id, and value for the custom fields "APPT 7" and "APPT FUT".
    """
    # TODO: abstract and decouple this function
    jobs = []
    for task in tasks:
        for custom_field in task["custom_fields"]:
            # check if the custom field name is location_id
            if custom_field["name"].lower() == "location id":  # and custom_field.get("value", None) is not None
                location_id = custom_field.get("value", None)
                # check if the location id is in the appts dict
                try:
                    data = count_appointments(appts[location_id])
                    jobs.append((task, custom_fields["APPT 7"], len(data["APPT 7"])))
                    jobs.append((task, custom_fields["APPT FUT"], len(data["APPT FUT"])))
                # if the location id is not in the appts dict, set the data to an empty list
                except KeyError:
                    data = {"APPT 7": [], "APPT FUT": []}
                    jobs.append((task, custom_fields["APPT 7"], None))
                    jobs.append((task, custom_fields["APPT FUT"], None))

    return jobs


def create_clickup_facebook_jobs(tasks, custom_fields, retailer_data):
    # I need to be able to create jobs for each retailer id one at a time
    jobs = []
    skeleton = {
        "date_preset": {"spend": None, "actions": {"lead": None, "purchase": None}},
        "ads_with_issues": {},
    }

    for task in tasks:
        for custom_field in task["custom_fields"]:
            if custom_field["name"].lower() == "id":
                retailer_id = custom_field.get("value", "")
                last_3d = retailer_data["last_3d"].get(retailer_id, skeleton["date_preset"])
                last_7d = retailer_data["last_7d"].get(retailer_id, skeleton["date_preset"])
                last_30d = retailer_data["last_30d"].get(retailer_id, skeleton["date_preset"])
                # error with ads with issues

                ads_with_issues = retailer_data["ads_with_issues"].get(retailer_id, skeleton["ads_with_issues"])
                jobs.extend(
                    [
                        (task, custom_fields["Spend 3"], last_3d.get("spend", None)),
                        (task, custom_fields["Spend 7"], last_7d.get("spend", None)),
                        (task, custom_fields["Spend 30"], last_30d.get("spend", None)),
                        (task, custom_fields["Leads 3"], last_3d.get("actions", {}).get("lead", None)),
                        (task, custom_fields["Leads 7"], last_7d.get("actions", {}).get("lead", None)),
                        (task, custom_fields["Leads 30"], last_30d.get("actions", {}).get("lead", None)),
                        (task, custom_fields["CPL 3"], last_3d.get("cpl", None)),
                        (task, custom_fields["CPL 7"], last_7d.get("cpl", None)),
                        (task, custom_fields["CPL 30"], last_30d.get("cpl", None)),
                        (task, custom_fields["CPP 3"], last_3d.get("cpp", None)),
                        (task, custom_fields["CPP 7"], last_7d.get("cpp", None)),
                        (task, custom_fields["CPP 30"], last_30d.get("cpp", None)),
                        (task, custom_fields["Issues"], len(ads_with_issues)),
                    ]
                )
    return jobs


def process_clickup_jobs(clickup_client, jobs):
    """
    Executes a batch of jobs by updating custom fields in a task using a `ClickupClient` object.

    Args:
        clickup_client (object): A `ClickupClient` object with the class attributes `RATE_LIMIT_REMAINING` and `RATE_RESET`.
        jobs (list): A list of tuples, where each tuple contains the task, custom field id, and value for the custom field.

    Returns:
        dict: A dictionary with the following keys:
            - `failures` (list): A list of tuples, where each tuple represents a job that failed to execute.
    """
    complete = False
    ret = {
        "failures": [],
    }
    jobs_ran = 0
    while not complete:
        clickup_client.refresh_rate_limit()
        rate = int(clickup_client.RATE_LIMIT_REMAINING)
        # run jobs number of jobs equal to the rate limit
        if jobs_ran + rate > len(jobs):
            rate = len(jobs) - jobs_ran
            complete = True
        jobs_to_run = jobs[jobs_ran : jobs_ran + rate]
        jobs_ran += rate
        _, failure = run_async_jobs(jobs_to_run, run_clickup_field_job)
        # print jobs ran and jobs left
        print(f"Jobs ran: {jobs_ran}, Jobs left: {len(jobs) - jobs_ran}")
        ret["failures"].extend(failure)
        # wait until the rate limit is reset
        rate_reset = float(clickup_client.RATE_RESET)
        sleep = rate_reset - time.time()
        if sleep > 0:
            time.sleep(rate_reset - time.time())
    return ret


def run_clickup_field_job(job):
    """
    Update the custom field for a task in Clickup.

    Args:
    - job (tuple): A tuple containing the following elements:
        - task (dict): A dictionary representing a task in Clickup.
        - custom_field_id (str): The id of the custom field to update.
        - value (str): The value to set the custom field to.

    Returns:
    - None

    """
    job[0].update_custom_field(custom_field_id=job[1], value=job[2])
    return


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
    Retrieve appointments from GHL client based on location and calendar.

    Parameters:
        - ghl_client (object): An object representing a GHL client.
        - clickup_task_list (list): A list of tasks from ClickUp.
        - start_day_delta (int): The number of days before or after today to start retrieving appointments.
        - end_day_delta (int): The number of days before or after today to end retrieving appointments.

    Returns:
        - appointment_data (dict): A dictionary containing appointments grouped by location, where the key is the location's id and the value is a dictionary with appointments information.
    """
    # TODO: Decouple checking if the location id matches tasks custom fields
    # TODO: implement threading somehow
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
                appointment_data[location["id"]] = []
                for calendar in calendars:
                    appointments = calendar.get_appointments(params=appointment_params)
                    appointment_data[location["id"]].extend(appointments)
    return appointment_data


def custom_field_dict(custom_field_list):
    # TODO: Abstract this
    """
    Create a dictionary of custom fields from a list of custom fields.

    Parameters:
        - custom_field_list (list): A list of clickup custom fields.

    Returns:
        - custom_field_dict (dict): A dictionary of custom fields where the keys are the names of the custom fields and the values are their corresponding ids.
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


def facebook_data_organized_by_date_preset(accounts, date_presets):
    """
    Retrieves data for a variety of date presets for a Facebook business and returns the result organized by date preset.

    Parameters:
        - accounts (list): A list of facebook AdAccount objects to query data from.
        - date_presets (list): A list of date presets to use when generating ad reports.

    Returns:
        - data (dict): A dictionary containing the following keys:
        - date_preset: a string representing the date preset used when generating ad reports.
            - "unsuccessful_reports": a list of ad reports that could not be processed.
            - "unsuccessfully_written": a list of ad reports that could not be written to the list passed to process_report
            - "unsuccessful_request_futures": a list of ad account requests that could not be processed.
            - "uncompleted_report_futures": a list of ad report jobs that could not be completed.
            - "data": the result of the ads
    """
    data = {}
    for presets in date_presets:
        data[presets] = facebook_ad_accounts_ad_data(accounts, presets)
    return data


def create_insights_params(
    date_preset,
):
    # ads the date_preset to the global DEFAULT_INSIGHTS_PARAMS object
    insights_params = copy.deepcopy(globals.DEFAULT_INSIGHTS_PARAMS)
    insights_params["date_preset"] = date_preset
    return insights_params


def proccess_report(report, list):
    """
    Adds the results of a Facebook AdReportRun object to a given list.

    Parameters:
        - report (AdReportRun): A Facebook AdReportRun object.
        - result_list (list): A list to which the report results will be added.

    The function retrieves the results of the `report` parameter by calling `report.get_result` with a limit of 500 results. The account rate limit information is also written to `globals.RATE_LIMIT` by calling `write_account_limits` with the headers from the result cursor. The results are then added to `result_list` and a message is printed indicating the successful write.
    """
    # TODO: I will want to implement try and except to catch facebook_errors
    cursor = report.get_result(params={"limit": 500})
    # write accounts rate limit to globals.RATE_LIMIT
    write_facebook_rate_limits(cursor.headers())
    list.extend(cursor)
    # TODO: log
    # print(f"Account: {report[AdReportRun.Field.account_id]} written")
    return


def organize_reports(reports):
    """
    Organizes Facebook AdReportRun objects into successful and unsuccessful reports.

    Parameters:
        - reports (list): A list of Facebook AdReportRun objects.

    Returns:
        A tuple of two elements:
            - successful_reports (list): A list of successful Facebook AdReportRun objects.
            - unsuccessful_reports (list): A list of unsuccessful Facebook AdReportRun objects.

    The function separates the Facebook AdReportRun objects in `reports` into two lists, one for successful reports and one for unsuccessful reports, based on the value of the `async_status` field. The two lists are returned as a tuple.
    """
    successful_reports = [
        report for report in reports if report[AdReportRun.Field.async_status].lower() == "job completed"
    ]
    unsuccessful_reports = [
        report for report in reports if report[AdReportRun.Field.async_status].lower() != "job completed"
    ]
    return successful_reports, unsuccessful_reports


def facebook_data_organized_by_regex(list_of_objs, target_key, regex_pattern):
    """
    filters a list of ad insights
    """
    ret = {}
    pattern = re.compile(regex_pattern)
    for obj in list_of_objs:
        extracted = extract_regex_expression(obj[target_key], pattern)
        if extracted:
            if extracted not in ret.keys():
                ret[extracted] = [obj]
            else:
                ret[extracted].append(obj)
    return ret


def consolidate_ad_stats(ads, target_key, regex_pattern):
    """
    Consolidates the statistics for a list of Facebook ad objects based on a regular expression pattern.

    Parameters:
        - ads (list): A list of Facebook ad objects.
        - target_key (str): The key for the ad object that will be used to extract the retailer id using the regular expression pattern.
        - regex_pattern (str): A regular expression pattern used to extract the retailer id from the target_key.

    The function creates a defaultdict, `consolidated_stats`, where the keys are the retailer ids and the values are dictionaries with the keys "spend" and "actions". The total spend and actions for each ad object with the same retailer id are added to the corresponding retailer id in `consolidated_stats`. The function uses the `extract_regex_expression` function to extract the retailer id from the target_key of each ad object and uses the regular expression pattern provided in `regex_pattern`.
    """
    # just learned about defaultdicts, this is a great use case for them
    consolidated_stats = defaultdict(lambda: {"spend": 0, "actions": {}})
    pattern = re.compile(regex_pattern)
    for ad in ads:
        retailer_id = extract_regex_expression(ad.get(target_key), pattern)
        if not retailer_id:
            continue
        consolidated_stats[retailer_id]["spend"] += float(ad.get("spend", 0))
        actions = ad.get("actions", [])
        for action in actions:
            action_type = action.get("action_type")
            if not action_type:
                continue
            if action_type not in consolidated_stats[retailer_id]["actions"]:
                consolidated_stats[retailer_id]["actions"][action_type] = 0
            consolidated_stats[retailer_id]["actions"][action_type] += int(action.get("value", 0))
    for retailer_id, stats in consolidated_stats.items():
        stats["cpl"] = safe_divide(stats["spend"], stats["actions"].get("lead", 0))
        stats["cpp"] = safe_divide(stats["spend"], stats["actions"].get("purchase", 0))
    return dict(consolidated_stats)


def safe_divide(numerator, denominator):
    try:
        return numerator / denominator
    except ZeroDivisionError:
        return None


def extract_regex_expression(string, expression):
    """
    Extracts a substring from `string` using a regular expression `expression`.

    Parameters:
        - string (str): The input string from which to extract a substring.
        - expression (str): A string representing a regular expression that will be used to extract a substring from `string`.

    Returns:
        - match (str or None): If a match is found, the extracted substring is returned. Otherwise, `None` is returned.
    """
    match = re.search(expression, string)
    if match:
        return match.group()
    else:
        return None


# TODO: Encapulate this function into a class
def facebook_ad_accounts_ad_data(ad_accounts, date_preset):
    """
    Parameters:
    - ad_accounts (list): A list of Facebook ad account objects.
    - date_preset (str): The date preset to use for retrieving ad account information.

    Returns:
        - ret (dict): A dictionary containing information about the ad account information retrieval process.
        The keys of the dictionary are:
            - "successful_reports": A list of successful reports for the ad accounts.
            - "unsuccessful_reports": A list of unsuccessful reports for the ad accounts.
            - "unsuccessful_request_futures": A list of futures representing unsuccessful requests.
            - "uncompleted_report_futures": A list of futures representing uncompleted reports.
            - "ads_data": A list of ad data for the ad accounts.
            - "unsuccessful_writes": A list of unsuccessful writes to process the ad data.
    """
    reports = create_reports(ad_accounts, date_preset)
    retry_accounts = [
        AdAccount(f"act_{report['account_id']}").api_get(fields=["account_id"])
        for report in reports["unsuccessful_reports"]
    ]
    if retry_accounts:
        print("Retrying creating reports for:")
        for accounts in retry_accounts:
            print(f"     {accounts['account_id']}")
    retry_reports = create_reports(retry_accounts, date_preset)
    # I don't want to merge the original reports unsuccessful reports, some of them might have been successful the second time
    del reports["unsuccessful_reports"]
    merged = merge_dicts(reports, retry_reports)

    ads = []
    # TODO: check write errors
    _, unsuccessful_writes = run_async_jobs(merged["successful_reports"], proccess_report, ads)
    merged["ads_data"] = ads
    merged["unsuccesful_writes"] = unsuccessful_writes
    return merged


def create_reports(ad_accounts, date_preset):
    """
    Create reports for multiple ad accounts.

    Parameters:
        - ad_accounts (list): A list of ad accounts to create reports for.
        - date_preset (str): The date preset to use when creating the reports.

    Returns:
        - ret (dict): A dictionary containing information about the report creation process.
        The keys of the dictionary are:
        - "successful_reports":A list of reports that were successful.
        - "unsuccessful_reports": A list of reports that were not successful.
        - "unsuccessful_request_futures": A list of futures that resulted in an unsuccessful request.
        - "uncompleted_report_futures": A list of futures that have not yet completed.
    """
    insights_params = create_insights_params(date_preset)
    # generate list of ad reports using threads
    # TODO: implement a way to check futures errors
    succesfully_created_reports, unsuccessful_request_futures = run_async_jobs(
        ad_accounts, create_async_job, globals.DEFAULT_INSIGHTS_FIELDS, insights_params
    )
    completed_reports, uncompleted_report_futures = run_async_jobs(succesfully_created_reports, wait_for_job)
    successful_reports, unsuccessful_reports = organize_reports(completed_reports)
    ret = {
        "successful_reports": successful_reports,
        "unsuccessful_reports": unsuccessful_reports,
        "unsuccessful_request_futures": unsuccessful_request_futures,
        "uncompleted_report_futures": uncompleted_report_futures,
    }
    return ret


def log_report_errors(facebook_data):
    """
    Logs errors for reports that were not successful.

    Parameters:
        - reports (list): A list of reports that were not successful.
    """
    for date_presets in facebook_data["data"].keys():
        if len(facebook_data["data"][date_presets]["unsuccessful_request_futures"]) > 0:
            print(f"Unsuccessful requests for {date_presets}:")
            for future in facebook_data["data"][date_presets]["unsuccessful_request_futures"]:
                print(f"    {future.exception()}")
            print("")
        if len(facebook_data["data"][date_presets]["unsuccessful_reports"]) > 0:
            print(f"Unsuccessful reports for {date_presets}:")
            for report in facebook_data["data"][date_presets]["unsuccessful_reports"]:
                print(f"    {report}")
            print("")


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


def ads_with_issues(accounts):
    """
    Retrieve advertisements with issues and organize the data.

    Parameters:
        - accounts (list): A list of Facebook account objects.
        - target_key (str, optional): The key to target when organizing the data.
        - regex_pattern (str, optional): The pattern to use when organizing the data with the target_key.

    Returns:
        - ret (dict): A dictionary containing the data and unsuccessful requests.
        - data (list or dict): A list of advertisements or a dictionary organized by a regex pattern based on the target_key.
        - unsuccessful_request (list): A list of unsuccessful requests.
    """
    ret = {}
    successful_requests, unsuccessful_request = run_async_jobs(
        accounts, request_issues, globals.ISSUES_FIELDS_PARAMS["fields"], globals.ISSUES_FIELDS_PARAMS["params"]
    )
    ads = []
    for lists in successful_requests:
        for ad in lists:
            if ad["configured_status"] != "ACTIVE":
                continue
            ads.append(ad)
    ret["data"] = ads
    ret["unsuccessful_request"] = unsuccessful_request
    return ret


def write_facebook_rate_limits(facebook_headers):
    """
    Write the Facebook rate limit information to a global dictionary.

    Parameters:
        - facebook_headers (dict): A dictionary of headers from a Facebook API response.

    Returns:
        - None
    """
    # list(globals.FACEBOOK_RATES.keys()) to edit while iterating dict keys
    for facebook_rate_signifiers in list(globals.FACEBOOK_RATES.keys()):
        if facebook_rate_signifiers in facebook_headers.keys():
            if facebook_rate_signifiers == "x-business-use-case-usage":
                account_info = json.loads(facebook_headers["x-business-use-case-usage"])
                for account_id in account_info.keys():
                    globals.FACEBOOK_RATES[facebook_rate_signifiers][account_id] = account_info[account_id]
            else:
                globals.FACEBOOK_RATES[facebook_rate_signifiers] = json.loads(
                    facebook_headers[facebook_rate_signifiers]
                )


def run_async_jobs(jobs, job_fn, *args, **kwargs):
    """
    Runs a list of jobs asynchronously using a thread pool executor.

    Parameters:
        - jobs (iterable): An iterable of jobs to be run.
        - job_fn (callable): A callable object that represents the job to be run.
        - *args: Positional arguments to be passed to `job_fn`.
        - **kwargs: Keyword arguments to be passed to `job_fn`.

    Returns:
        A tuple of two elements:
            - results (list): A list of results, where each result is the return value of `job_fn` for a single job.
            - failed_jobs (list): A list of jobs that failed to complete.

    The function creates a `ThreadPoolExecutor` and submits a list of jobs to it, where each job is a call to `job_fn` with the corresponding job and any additional arguments passed in `*args` and `**kwargs`. The function then waits for all jobs to complete and collects the results, as well as any jobs that failed to complete. The results and failed jobs are returned as a tuple.
    """
    results = []
    failed_jobs = []
    with ThreadPoolExecutor() as executor:
        job_futures = [executor.submit(job_fn, job, *args, **kwargs) for job in jobs]
        for future in as_completed(job_futures):
            try:
                result = future.result()
                results.append(result)
            except:
                failed_jobs.append(future)
    return results, failed_jobs


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
    # TODO: use logging module
    # print(f"Created report for account: {account['id']}")
    return job.api_get()


def wait_for_job(job):
    """
    Waits for a Facebook AdReportRun job to complete and returns the results.

    Parameters:
        - job (AdReportRun): A Facebook AdReportRun object representing the job to wait for.

    Returns:
        A Facebook AdReportRun object representing the completed job, including the results.

    The function retrieves the status of the job every 10 seconds until the job is marked as complete or failed. If the job fails, the current status of the job is returned. If the job is successful, the function retrieves the final result of the job and returns it.
    """
    job = job.api_get()
    account_id = job["account_id"]
    # TODO: use logging module
    # print(f"Waiting for account: {account_id}")
    while (
        job[AdReportRun.Field.async_percent_completion] < 100 or job[AdReportRun.Field.async_status] == "Job Running"
    ):
        if job[AdReportRun.Field.async_status] == "Job Failed":
            return job
        time.sleep(10)
        job = job.api_get()
    # string = f"ID:{account_id} Type:result"
    # print(string)
    return job.api_get()


def write_object_structure(object):
    with open(f"structures/{object.__class__.__name__}.json", "w") as file:
        file.write(json.dumps(object._data, indent=4))


def count_objects(d):
    count = 0
    if isinstance(d, dict):
        for value in d.values():
            count += count_objects(value)
    elif isinstance(d, list):
        for value in d:
            count += count_objects(value)
    else:
        count = 1
    return count


def write_all_data_to_csv(facebook_data, appointments):
    for date_preset in facebook_data["data"]:
        write_ads_to_csv(facebook_data["data"][date_preset]["ads_data"], f"uploads/{date_preset}.csv")
    write_ads_with_issues_to_csv(facebook_data["ads_with_issues"]["data"], "uploads/ads_with_issues.csv")
    write_appointments_to_csv(appointments, "uploads/appointments.csv")


def write_appointments_to_csv(appointments, filename):
    fieldsnames = set()
    fieldsnames.add("location_id")
    for appts in appointments.values():
        for appt in appts:
            fieldsnames.update(appt.keys())
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(fieldsnames))
        writer.writeheader()
        for location_id, appointment in appointments.items():
            for appt in appointment:
                appt["location_id"] = location_id
                writer.writerow(appt)


def write_ads_to_csv(data, filename):
    """
    Write ad data to a CSV file, with additional columns for extracted information.

    Parameters:
        - data (list of dict): A list of dictionaries, where each dictionary represents ad data.
        - filename (str): The name of the CSV file to write to.

    Returns:
        - None

    The function extracts the retailer ID and creative ID from the ad name and adds them as new columns to the CSV file. The function also extracts the number of leads and purchases from the "actions" key in each ad's dictionary and adds them as new columns to the CSV file. All other data from the ad dictionaries is written to the CSV file as-is.

    """

    # use a set comprehension to get the keys from each dictionary
    key_sets = [set(d.keys()) for d in data]
    # use the union method to combine all of the key sets into a single set
    unique_keys = set().union(*key_sets)
    unique_keys.discard("actions")
    unique_keys.add("leads")
    unique_keys.add("purchases")
    unique_keys.add("creative_id")
    unique_keys.add("retailer_id")
    fieldsnames = sorted(list(unique_keys))
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldsnames)
        writer.writeheader()
        for d in data:
            row = d.export_all_data().copy()
            row["retailer_id"] = extract_regex_expression(d["ad_name"], r"^\d{3}")
            row["creative_id"] = extract_regex_expression(d["ad_name"], r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+")
            if "actions" in d:
                for a in d["actions"]:
                    if a["action_type"] == "lead":
                        row["leads"] = a["value"]
                    if a["action_type"] == "purchase":
                        row["purchases"] = a["value"]
                del row["actions"]
            writer.writerow(row)


def write_ads_with_issues_to_csv(data, filename):
    fieldnames = set.union(*[set(d.keys()) for d in data])

    with open("uploads/ads_with_issues.csv", "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for d in data:
            writer.writerow(d.export_all_data())


def upload_to_clickup(tasks):
    uploads = ["ads_with_issues", "last_7d", "last_3d", "last_30d", "appointments"]
    timestamp = datetime.now().strftime("%Y-%m-%d")
    for task in tasks:
        if task["name"] in uploads:
            print("Uploading file to ClickUp: ", task["name"])
            file = {"attachment": (f"{timestamp}.csv", open(f"uploads/{task['name']}.csv", "rb"))}
            task.upload_file(file)


def write_dicts_to_json(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f, default=lambda x: x.export_all_data(), indent=4)


def merge_dicts(dict1, dict2):
    combined_dict = {}
    keys = set([*dict1.keys(), *dict2.keys()])
    for key in keys:
        combined_dict[key] = dict1.get(key, []) + dict2.get(key, [])
    return combined_dict


def request_issues(account, fields, params):
    """
    Retrieve advertising issues from a Facebook account.

    Parameters:
        - account (dict): A dictionary containing information about a Facebook account.
        - fields (str): A string representing the fields to be included in the request.
        - params (dict): A dictionary containing parameters for the request.

    Returns:
        - issues (object): An object representing the issues retrieved from the Facebook account.
    """
    account_id = account["id"]
    # TODO: LOG
    # print(f"Requesting Issues for account: {account_id}")
    issues = account.get_ads(fields=fields, params=params)
    write_facebook_rate_limits(issues.headers())
    # print(f"Issues for account {account_id}")
    return issues
