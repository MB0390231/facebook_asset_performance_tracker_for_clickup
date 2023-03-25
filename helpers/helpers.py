from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import csv
import re
import csv
import datetime
from helpers.logging_config import BaseLogger

logger = BaseLogger(name="helpers")


def convert_timestamp_to_date(timestamp):
    date = datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")
    return date


# REGEX
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


def extend_list_async(cursor, list):
    len_before = len(list)
    if cursor.__class__.__name__ != "Cursor":  # assume adreport run
        cursor = cursor.get_result(params={"limit": 300})
    with Lock():
        list.extend(cursor)
    len_after = len(list)
    logger.logger.debug(f"Extended list from {len_before} to {len_after}")
    return None


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
        job_futures = {executor.submit(job_fn, job, *args, **kwargs): job for job in jobs}
        for future in as_completed(job_futures):
            job = job_futures[future]
            try:
                result = future.result()
                success.append(result)
            except Exception as e:
                logger.logger.exception(
                    f"Error executing Future:\n"
                    f"job: {job}\n"
                    f"job_fn: {job_fn.__name__}\n"
                    f"args: {args}\n"
                    f"kwargs: {kwargs}\n"
                    f"{'-'*80}\n",
                    exc_info=e,
                )
                fail.append((job, job_fn, args, kwargs))
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

    unique_keys = set().union(*(set(d.keys()) for d in data))
    unique_keys -= {"actions"}
    unique_keys |= {"leads", "purchases", "creative_id", "retailer_id", "copy_id"}
    # add action types from actions to unique_keys
    for d in data:
        for action in d.get("actions", []):
            unique_keys.add(action["action_type"])

    fieldsnames = sorted(list(unique_keys))
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldsnames)
        writer.writeheader()
        for d in data:
            row = d.copy()
            row["retailer_id"] = extract_regex_expression(d["ad_name"], r"^\d{3}")
            row["creative_id"] = extract_regex_expression(d["ad_name"], r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+")
            row["copy_id"] = extract_regex_expression(d["ad_name"], r"ADC#[a-zA-Z0-9]+")
            for action in d.get("actions", []):
                row[action["action_type"]] = action["value"]
            if "actions" in row:
                del row["actions"]
            writer.writerow(row)

    return None


def current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def logs_date():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def upload_to_clickup(tasks):
    # generate a timestamp with the current date and time in the format YYYY-MM-DD HH:MM am (e.g. 2020-01-01 12:00 am)
    for task in tasks:
        if task["name"].lower() == "runtime":
            file = {"attachment": (f"{current_date()}.log", open(f"logs/{current_date()}.log", "rb"))}
            task.upload_file(file)
            print("Uploading runtime log")
            continue
        try:
            file = {"attachment": (f"{current_date()}.csv", open(f"uploads/{task['name']}.csv", "rb"))}
            task.upload_file(file)
            print(f"Uploaded file for task {task['name']}")
        except Exception as e:
            print(f"No file to upload for task {task['name']}")
