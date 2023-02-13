from datetime import datetime, timedelta
from exceptions import ClickupObjectNotFound
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
import time, json, re, globals, copy
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


def facebook_data_organized_by_date_preset(business, date_presets, data_processor=None):
    """
    Retrieves data at for a variety of date presets. Returns a dictionary of ad data with the following format. It is organized by date presets

    return format = {
       "date_preset":{

    }
    """
    ad_accounts = facebook_business_active_ad_accounts(business=business)
    data = {}
    for presets in date_presets:
        data[presets] = facebook_ad_accounts_ad_data(ad_accounts, presets, data_processor)
    return data


def facebook_ad_accounts_ad_data(ad_accounts, date_preset, data_processor):
    """
    Generates ad reports for a list of Facebook ad accounts, processes the data, and returns the result.

    Parameters:
        - ad_accounts (list): A list of Facebook ad accounts for which ad reports should be generated.
        - date_preset (str): A string representing the date preset to use when generating ad reports.
        - data_processor (callable, optional): A callable object that will process the generated ad reports. If not provided, the processed ad reports will be stored in the returned dictionary under the key "data".

    Returns:
        - ret (dict): A dictionary containing the following keys:
        - "unsuccessful_reports": a list of ad reports that could not be processed.
        - "unsuccessfully_written": a list of ad reports that could not be written to the list passed to process_report
        - "unsuccessful_request_futures": a list of ad account requests that could not be processed.
        - "uncompleted_report_futures": a list of ad report jobs that could not be completed.
        - "data": the result of calling data_processor on the generated ad reports, or the processed ads
    """
    insights_params = create_insights_params(date_preset)
    # generate list of ad reports using threads
    succesfully_created_reports, unsuccessful_request_futures = run_async_jobs(
        ad_accounts, create_async_job, globals.DEFAULT_INSIGHTS_FIELDS, insights_params
    )
    completed_reports, uncompleted_report_futures = run_async_jobs(succesfully_created_reports, wait_for_job)
    # TODO: Retry with the unsuccessful reports
    successful_reports, unsuccessful_reports = organize_reports(completed_reports)
    ads = []
    successfully_written, unsuccessfully_written = run_async_jobs(successful_reports, proccess_report, ads)
    ret = {
        "unsuccessful_reports": unsuccessful_reports,
        "unsuccessfully_written": unsuccessfully_written,
        "unsuccessful_request_futures": unsuccessful_request_futures,
        "uncompleted_report_futures": uncompleted_report_futures,
    }
    if data_processor is None:
        ret["data"] = ads
    else:
        ret["data"] = data_processor(ads)
    return ret


def create_insights_params(date_preset):
    # ads the date_preset to the global DEFAULT_INSIGHTS_PARAMS object
    insights_params = copy.deepcopy(globals.DEFAULT_INSIGHTS_PARAMS)
    insights_params["date_preset"] = date_preset
    return insights_params


def proccess_report(report, list):
    """I want the function to add the report results to a given list"""
    # TODO: I will want to implement try and except to catch facebook_errors
    cursor = report.get_result(params={"limit": 500})
    # write accounts rate limit to globals.RATE_LIMIT
    write_account_limits(cursor.headers())
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


def ads_retailer_id_processor(ads):
    pattern = r"^\d{3}"
    return facebook_data_organized_by_regex(ads, "ad_name", pattern)


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


def write_account_limits(headers):
    """
    Writes account usage information to the global `FB_RATES` dictionary.

    Parameters:
    - headers (dict): A dictionary containing header information, including the key "x-business-use-case-usage" with a JSON-encoded string value representing account usage information.

    Returns:
    - None

    The function extracts the account usage information from the headers and writes it to the `FB_RATES` dictionary, where the keys are account IDs and the values are the usage information for each account.
    """
    account_info = json.loads(headers["x-business-use-case-usage"])
    for account_id in account_info.keys():
        globals.FB_RATES[account_id] = account_info[account_id]


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
