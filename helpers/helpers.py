from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import csv
import re
import csv
import datetime
from helpers.logging_config import BaseLogger
import csv
from typing import Callable, Dict, List, Any


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


def write_dicts_to_csv(
    dict_list: List[Dict[str, Any]],
    fieldnames: List[str],
    generate_row: Callable[[Dict[str, Any]], Dict[str, Any]],
    file_name: str,
) -> None:
    """
    Write a list of dictionaries to a CSV file using the specified fieldnames and row generation function.

    Args:
        dict_list (List[Dict[str, Any]]): A list of dictionaries to write to the CSV file.
        fieldnames (List[str]): A list of fieldnames (column names) for the CSV file.
        generate_row (Callable[[Dict[str, Any]], Dict[str, Any]]): A function that takes a dictionary from dict_list and generates a row.
        file_name (str): The name of the CSV file to write the data to.
    """
    # Write dictionaries to a CSV file
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for d in dict_list:
            row = generate_row(d)
            writer.writerow(row)
    return None


def extract_action_types_and_values(d: Dict[str, Any]) -> Dict[str, Any]:
    actions = {}
    for action in d.get("actions", []):
        if action["action_type"] in ["lead", "purchase"]:
            actions[action["action_type"]] = action["value"]
    return actions


def generate_reporting_row(d: Dict[str, Any]) -> Dict[str, Any]:
    row = {key: d[key] for key in d if key != "actions"}
    row.update(extract_action_types_and_values(d))
    return row


def generate_rejected_ad_row(d: Dict[str, Any]) -> Dict[str, Any]:
    row = {key: d[key] for key in d}
    row.update(extract_assets(d["name"]))
    return row


def extract_assets(name: str) -> Dict[str, str]:
    asset_mapping = {
        "retailer_id": extract_regex_expression(name, r"^\d{3}"),
        "creative_id": extract_regex_expression(name, r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+"),
        "copy_id": extract_regex_expression(name, r"ADC#[a-zA-Z0-9]+"),
    }
    return asset_mapping


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
