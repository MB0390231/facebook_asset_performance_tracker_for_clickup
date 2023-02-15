from datetime import datetime, timedelta
from exceptions import ClickupObjectNotFound
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
import time, json, re, globals, copy, csv
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
                appointment_data[location["id"]] = {"appointments": []}
                for calendar in calendars:
                    appointments = calendar.get_appointments(params=appointment_params)
                    appointment_data[location["id"]]["appointments"].extend(appointments)
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
    print(f"Account: {report[AdReportRun.Field.account_id]} written")
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
    retry_accounts = [AdAccount(f"act_{report['account_id']}") for report in reports["unsuccessful_reports"]]
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
        ads.extend(lists)
    ret["data"] = ads
    ret["unsuccessful_request"] = unsuccessful_request
    return ret


# def retry_failed_reports(failed_reports, reuse_list, date_preset):
#     """
#     This function runs through a list failed ad reports, creates a new report for the report["account_id"],
#     and if the new report is successful it retrieves the data and adds it to the reuse_list
#     """
#     accounts = [AdAccount(f"act" + report["account_id"]) for report in failed_reports]
#     insights_params = create_insights_params(date_preset)
#     succesfully_created_reports, unsuccessful_request_futures = run_async_jobs(
#         accounts, create_async_job, globals.DEFAULT_INSIGHTS_FIELDS, insights_params
#     )

#     # generate list of ad reports using threads
#     completed_reports, uncompleted_report_futures = run_async_jobs(succesfully_created_reports, wait_for_job)
#     successful_reports, unsuccessful_reports = organize_reports(completed_reports)
#     _, unsuccessful_writes = run_async_jobs(successful_reports, proccess_report, reuse_list)
#     ret = {
#         "retry_data": {
#             "unsuccessful_reports": unsuccessful_reports,
#             "unsuccessful_writes": unsuccessful_writes,
#             "unsuccessful_request_futures": unsuccessful_request_futures,
#             "uncompleted_report_futures": uncompleted_report_futures,
#         }
#     }
#     return ret


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
                    globals.FACEBOOK_RATES[account_id] = account_info[account_id]
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
    print(f"Created report for account: {account['id']}")
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


def write_to_csv(objects, filename, fieldnames=None, default=None):
    """
    Write a list of dictionaries to a CSV file.

    Parameters:
        - data (list): A list of dictionaries where each dictionary represents a row in the CSV file.
        - fieldnames (list): A list of field names that correspond to the keys in the dictionaries.
        - filename (str): The name of the CSV file to write to.
        - default_value (str): A default value to use for dictionaries that do not have a key corresponding to one of the fieldnames.

    The function writes the list of dictionaries in data to a CSV file named filename. The field names in the CSV file are specified in the fieldnames parameter. If a dictionary in data is missing a key corresponding to one of the fieldnames, the default_value is used instead. The function returns nothing.
    """
    if fieldnames is None:
        fieldnames = set().union(*(obj.keys() for obj in objects))
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for obj in objects:
            row = {field: obj.get(field, default) for field in fieldnames}
            writer.writerow(row)


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
    print(f"Requesting Issues for account: {account_id}")
    issues = account.get_ads(fields=fields, params=params)
    write_facebook_rate_limits(issues.headers())
    print(f"Issues for account {account_id}")
    return issues
