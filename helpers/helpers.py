from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import re
import csv
import datetime


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
            row = d.copy()
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


def current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def weighted_average(values, weights):
    "order matters regarding values and weights"
    if len(values) != len(weights):
        raise ValueError("The number of values must be equal to the number of weights")
    if sum(weights) == 0:
        raise ValueError("The sum of weights cannot be zero")
    return sum([values[i] * weights[i] for i in range(len(values))]) / sum(weights)


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
