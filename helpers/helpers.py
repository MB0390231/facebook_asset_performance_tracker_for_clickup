from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import csv
import re
import csv
import datetime
from helpers.logging_config import BaseLogger
import csv
from time import mktime, sleep
from typing import Callable, Dict, List, Any, Optional, Union
from datetime import timedelta, datetime

logger = BaseLogger(name="helpers")


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


def datetime_to_epoch(datetime_string):
    """Converts a datetime string to an epoch timestamp.

    Arguments:
        - datetime_string -- A string in the format "YYYY-MM-DD HH:MM:SS".

    Returns:
        - An integer representing the number of seconds elapsed since 1970-01-01 00:00:00 (the epoch).
    """
    datetime_obj = datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
    return int(datetime_obj.timestamp() * 1000)


def week_ago_epoch_timestamp():
    now = datetime.datetime.now()
    week_ago = now - datetime.timedelta(weeks=1)
    epoch_week_ago = int(mktime(week_ago.timetuple()))
    return epoch_week_ago


def convert_timestamp_to_date(timestamp):
    date = datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")
    return date


def current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def logs_date():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# REGEX
def extract_regex_expression(string: str, expression: str) -> Optional[str]:
    """
    Extracts a substring from `string` using a regular expression `expression`.

    Args:
        string (str): The input string from which to extract a substring.
        expression (str): A string representing a regular expression that will be used to extract a substring from `string`.

    Returns:
        Optional[str]: If a match is found, the extracted substring is returned. Otherwise, `None` is returned.

    Example:
        string = "This is a sample string"
        expression = r"\b\w+\b"
        result = extract_regex_expression(string, expression)
        print(result)
        # Output: 'This'
    """
    pattern = re.compile(expression)
    match = re.search(pattern, string)
    if match:
        return match.group()
    else:
        return None


# THREADS


def extend_list_async(cursor, result_list: List) -> None:
    """
    Extends a list with the contents of a cursor asynchronously.

    Args:
    cursor (Union[Cursor, adreport.Cursor]): The cursor object to extract data from.
    result_list (List): The list to be extended with the contents of the cursor.

    Returns:
        None

    Example:
        result_list = []
        cursor = [1, 2, 3, 4, 5]
        extend_list_async(cursor, result_list)
        print(result_list)
        # Output: [1, 2, 3, 4, 5]
    """
    # try twice because facebook sometimes gives an error with no other information than "unknown error"
    try:
        len_before = len(result_list)
        with Lock():
            result_list.extend(cursor)
        len_after = len(result_list)
        logger.logger.debug(f"Extended list from {len_before} to {len_after}")
    except:
        sleep(1)
        len_before = len(result_list)
        with Lock():
            result_list.extend(cursor)
        len_after = len(result_list)
        logger.logger.debug(f"Extended list from {len_before} to {len_after}")
    return


def run_async_jobs(jobs, job_fn, *args, **kwargs):
    """
    Runs a list of jobs asynchronously using a thread pool.

    Args:
        jobs (List): A list of jobs to be executed.
        job_fn (Callable): A function that will be used to execute each job.
        *args: Arguments to be passed to `job_fn` for each job.
        **kwargs: Keyword arguments to be passed to `job_fn` for each job.

    Returns:
        Tuple[List, List]: A tuple containing two lists. The first list contains the results of the successful jobs. The second list contains the jobs that failed, represented as tuples of (job, job_fn, args, kwargs).

    Example:
        def job_fn(job):
            return job * 2

        jobs = [1, 2, 3, 4, 5]
        success, fail = run_async_jobs(jobs, job_fn)
        print(success)
        # Output: [2, 4, 6, 8, 10]
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
    Writes a list of dictionaries to a CSV file.

    Args:
        dict_list (List[Dict[str, Any]]): A list of dictionaries to be written to the CSV file.
        fieldnames (List[str]): The names of the fields for each row of the CSV file.
        generate_row (Callable[[Dict[str, Any]], Dict[str, Any]]): A function that takes in a dictionary and returns a dictionary, used to generate each row of the CSV file.
        file_name (str): The name of the CSV file to be written.

    Returns:
        None

    Example:
        dict_list = [{"field1": "value1", "field2": "value2"}, {"field1": "value3", "field2": "value4"}]
        fieldnames = ["field1", "field2"]
        generate_row = lambda x: x
        file_name = "example.csv"
        write_dicts_to_csv(dict_list, fieldnames, generate_row, file_name)

        # Output: Creates a file "example.csv" with the following contents:
        # field1,field2
        # value1,value2
        # value3,value4
    """
    with open(file_name, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for d in dict_list:
            row = generate_row(d)
            writer.writerow(row)
    return None


def extract_action_types_and_values(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts the action types "lead" and "purchases" and their values from a dictionary.

    Args:
        d (Dict[str, Any]): The dictionary from which to extract the action types and values.

    Returns:
        Dict[str, Any]: A dictionary containing the extracted action types and values.

    Example:
    d = {
        "actions": [
            {"action_type": "lead", "value": 1},
            {"action_type": "purchase", "value": 2},
            {"action_type": "view", "value": 3},
        ]
    }
    result = extract_action_types_and_values(d)
    print(result)
    # Output: {'lead': 1, 'purchase': 2}
    """
    actions = {}
    for action in d.get("actions", []):
        if action["action_type"] in ["lead", "purchase"]:
            actions[action["action_type"]] = action["value"]
    return actions


def generate_reporting_row(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates a reporting row from a dictionary.

    Args:
        d (Dict[str, Any]): The input dictionary from which to generate the reporting row.

    Returns:
        Dict[str, Any]: The generated reporting row.

    Example:
        d = {
            "spend": 20.0,
            "actions": [
                {"action_type": "lead", "value": 1},
                {"action_type": "purchase", "value": 2},
            ],
        }
        result = generate_reporting_row(d)
        print(result)
        # Output: {'spend': 20.0, 'lead': 1, 'purchase': 2}
    """
    row = {key: d[key] for key in d if key != "actions"}
    row.update(extract_action_types_and_values(d))
    return row


def generate_rejected_ad_row(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates a rejected ad row from a dictionary.

    Args:
        d (Dict[str, Any]): The input dictionary from which to generate the rejected ad row.

    Returns:
        Dict[str, Any]: The generated rejected ad row.

    Example:
        d = {
            "name": "123_VID#12345_ADC#67890",
            "spend": 20.0,
        }
        result = generate_rejected_ad_row(d)
        print(result)
        # Output: {'name': '123_VID#12345_ADC#67890', 'spend': 20.0, 'retailer_id': '123', 'creative_id': 'VID#12345', 'copy_id': 'ADC#67890'}
    """
    row = {key: d[key] for key in d}
    row.update(
        extract_assets(d["name"]),
        {
            "retailer_id": r"^\d{3}",
            "creative_id": r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+",
            "copy_id": r"ADC#[a-zA-Z0-9]+",
        },
    )
    return row


def extract_assets(name: str, assets: Dict[str, str]) -> Dict[str, str]:
    """
    Extracts assets from a given name based on a dictionary of asset regex patterns.

    Args:
        name (str): The string from which to extract assets.
        assets (Dict[str, str]): A dictionary containing asset names as keys and their regex patterns as values.

    Returns:
        Dict[str, str]: A dictionary containing the extracted assets as key-value pairs.

    Examples:
        assets = {
            "retailer_id": r"^\d{3}",
            "creative_id": r"VID#[a-zA-Z0-9]+|IMG#[a-zA-Z0-9]+",
            "copy_id": r"ADC#[a-zA-Z0-9]+",
        }
        extract_assets("001VID#abcdADC#efgh", assets)
        {'retailer_id': '001', 'creative_id': 'VID#abcd', 'copy_id': 'ADC#efgh'}
    """

    def extract_regex_expression(text: str, pattern: str) -> str:
        match = re.search(pattern, text)
        return match.group(0) if match else ""

    asset_mapping = {asset: extract_regex_expression(name, pattern) for asset, pattern in assets.items()}
    return asset_mapping


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
    return None
