from datetime import datetime, timedelta
from exceptions import ClickupObjectNotFound
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import time
import csv
import pytz
import sys
import math
import json
import re
import defaults
import copy
import csv


# DATETIME
def datetime_to_epoch(datetime_string):
    """Converts a datetime string to an epoch timestamp.

    Arguments:
        - datetime_string -- A string in the format "YYYY-MM-DD HH:MM:SS".

    Returns:
        - An integer representing the number of seconds elapsed since 1970-01-01 00:00:00 (the epoch).
    """
    datetime_obj = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    return int(datetime_obj.timestamp() * 1000)


def convert_timestamp_to_date(timestamp):
    date = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")
    return date


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


def convert_timestamp_to_timezone(timestamp_str, timezone_str):
    # Convert the timestamp string to a datetime object
    date = datetime.fromisoformat(timestamp_str)

    # Get the timezone object for the desired timezone
    time_zone = pytz.timezone(timezone_str)

    # Convert the datetime object to the desired timezone
    time_zone_date = date.astimezone(time_zone)

    # Format the datetime object as a string
    time_zone_date_str = time_zone_date.strftime("%Y-%m-%d %H:%M:%S")

    return time_zone_date_str


# CLICKUP
def create_clickup_jobs(tasks, custom_fields, ghl_appointments, facebook_data):
    appointment_jobs = create_ghl_clickup_field_jobs(tasks, custom_fields, ghl_appointments)
    facebook_jobs = create_clickup_facebook_jobs(tasks, custom_fields, facebook_data)
    return [*appointment_jobs, *facebook_jobs]


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


def get_clickup_folder(client, team_name, space_name, folder_name):
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
    try:
        found_folder = [folder for folder in found_spaces.get_folders() if folder["name"] == folder_name][0]
        return found_folder
    except IndexError:
        raise ClickupObjectNotFound(f"The Clickup Folder: {folder_name} was not found.")
    return


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


# GHL
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
    seven_days_ago = current_time - timedelta(days=7)

    for appointment in appointments:
        if appointment["appoinmentStatus"] == "confirmed":
            # must convert utc time to timezone time
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
        "includeAll": True,
    }
    locations = ghl_client.get_locations()
    appointment_data = {}
    for location in locations:
        for task in clickup_task_list:
            if location["id"] == get_custom_field_value(input_list=task["custom_fields"], name="Location ID"):
                calendars = location.get_calendar_services()
                appointment_data[location["id"]] = []
                for calendar in calendars:
                    try:
                        appointments = calendar.get_appointments(params=appointment_params)
                        # convert timestamps to timezone of appointment
                        for appointment in appointments:
                            try:
                                appointment["createdAt"] = convert_timestamp_to_timezone(
                                    appointment["createdAt"], appointment["selectedTimezone"]
                                )
                                appointment["startTime"] = convert_timestamp_to_timezone(
                                    appointment["startTime"], appointment["selectedTimezone"]
                                )
                            except KeyError:
                                print("KeyError: ", appointment["id"])
                        appointment_data[location["id"]].extend(appointments)
                    except Exception as e:
                        print("Exception: ", e)
    return appointment_data


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


def log_report_errors(facebook_data):
    """
    Logs errors for reports that were not successful.

    Parameters:
        - reports (list): A list of reports that were not successful.
    """
    for date_presets in facebook_data["data"].keys():
        target_data = facebook_data["data"][date_presets]
        if "unsuccessful_account_ids" in target_data.keys() and len(target_data["unsuccessful_account_ids"]) > 0:
            print(f"Unsuccessful retries for {date_presets}:")
            for account_id in target_data["unsuccessful_retries"]:
                print(f"    {account_id}")
        if (
            "unsuccessful_request_futures" in target_data.keys()
            and len(target_data["unsuccessful_request_futures"]) > 0
        ):
            print(f"Unsuccessful requests for {date_presets}:")
            for future in target_data["unsuccessful_request_futures"]:
                print(f"    {future.exception()}")
            print("")
        if "unsuccessful_reports" in target_data.keys() and len(target_data["unsuccessful_reports"]) > 0:
            print(f"Unsuccessful reports for {date_presets}:")
            for report in target_data["unsuccessful_reports"]:
                print(f"    {report}")
            print("")


# REGEX
def get_creative_id(str):
    return extract_regex_expression(str, r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+")


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
    pattern = re.compile(expression)
    match = re.search(pattern, string)
    if match:
        return match.group()
    else:
        return None


# THREADS


def run_async_jobs(jobs, job_fn, *args, **kwargs):
    """
    Run a list of jobs asynchronously using threads.

    Parameters:
        - jobs (list): A list of jobs to run.
        - job_fn (function): The function to run for each job.
        - *args (tuple): A tuple of arguments to pass to the job function.

    Returns:
        - tuple: A tuple containing a list of success and a list of fail.
    """
    success = []
    fail = []
    with ThreadPoolExecutor() as executor:
        job_futures = [executor.submit(job_fn, job, *args, **kwargs) for job in jobs]
        for future in as_completed(job_futures):
            try:
                result = future.result()
                success.append(result)
            except:
                fail.append(future)
    return success, fail


# WRITING/MISC
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


def write_object_structure(object):
    with open(f"structures/{object.__class__.__name__}.json", "w") as file:
        file.write(json.dumps(object._data, indent=4))


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
    unique_keys.add("copy_id")
    fieldsnames = sorted(list(unique_keys))
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldsnames)
        writer.writeheader()
        for d in data:
            row = d.export_all_data().copy()
            row["retailer_id"] = extract_regex_expression(d["ad_name"], r"^\d{3}")
            row["creative_id"] = extract_regex_expression(d["ad_name"], r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+")
            row["copy_id"] = extract_regex_expression(d["ad_name"], r"ADC#[a-zA-Z0-9]+")
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
    fieldnames.add("creative_id")
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for d in data:
            row = d.export_all_data().copy()
            row["creative_id"] = get_creative_id(d["name"])
            writer.writerow(row)


def write_dicts_to_json(data, file_path):
    with open(file_path, "w") as f:
        json.dump(data, f, default=lambda x: x.export_all_data(), indent=4)


def merge_dicts(dict1, dict2):
    # merges dictionaries with the same keys
    combined_dict = {}
    keys = set([*dict1.keys(), *dict2.keys()])
    for key in keys:
        combined_dict[key] = dict1.get(key, []) + dict2.get(key, [])
    return combined_dict


def sleep_and_print(seconds):
    """
    Sleeps for the given number of seconds and prints the number of seconds passed after each second.
    """
    for i in range(seconds, 0, -1):
        time.sleep(1)
        sys.stdout.write(f"\rSeconds passed: {i}")
        sys.stdout.flush()
    print("")


def loading_bar(progress, total, bar_length=40, time_to_sleep=None):
    percent_complete = progress / total
    num_bar_symbols = int(percent_complete * bar_length)
    bar = "[" + "-" * num_bar_symbols + " " * (bar_length - num_bar_symbols) + "]"

    sys.stdout.write("\r" + bar + " " + f"{progress}/{total} ({int(percent_complete) * 100}%)")
    sleep_and_print(time_to_sleep)
    # sys.stdout.flush()


def current_date():
    return datetime.now().strftime("%Y-%m-%d")


def weighted_average(values, weights):
    "order matters regarding values and weights"
    if len(values) != len(weights):
        raise ValueError("The number of values must be equal to the number of weights")
    if sum(weights) == 0:
        raise ValueError("The sum of weights cannot be zero")
    return sum([values[i] * weights[i] for i in range(len(values))]) / sum(weights)
