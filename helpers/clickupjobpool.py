from helpers.logging_config import get_logger
from helpers.helpers import run_async_jobs
import time
import math


class ClickupJobPool:
    """
    This class is going to handle the discovery of facebook insights fields to be used by the clickup job handler.
    Statistics Discovery:
    - Handle discovery of clickup stats from custom fields.
    - Discovery will use the following format as input:
        {
            "custom_fields_name": "custom_field_id",
            },
        }
    - It will use the custom field name to generate a list of insights fields that could be used to create jobs.
    - It will  return a mapping of custom field names to insights fields.

    - Create a standard format for custom fields and clickup jobs
    - Handle the creation of clickup jobs and clickup custom fields.
    - Handle the logging of clickup jobs and clickup custom fields.
    - Handle the updating of clickup jobs and clickup custom fields.
    """

    # mapping of custom field names to insights fields. Used to correlate custom fields to insights fields.
    insights_field_mapping = {
        "leads": ("actions", "lead"),
        "spend": "spend",
        "purchases": ("actions", "purchase"),
        "cpl": "cost_per_lead",
        "cpp": "cost_per_purchase",
        "cpc": "cost_per_inline_link_click",
        "cpm": "cpm",
        "ctr": "inline_link_click_ctr",
        "count": "count",
        "issues": "issues",
    }

    date_mapping = {
        "3": "last_3d",
        "7": "last_7d",
        "30": "last_30d",
        "max": "maximum",
    }

    def __init__(self, clickup_client):
        self.clickup_client = clickup_client
        self.jobs = []
        self.logger = get_logger(self.__class__.__name__)
        return super().__init__()

    def create_insights_jobs(
        self, task, custom_fields, asset_data, asset_match_type="name", custom_field_location=None
    ):
        """
        This method will take in a dictionary of custom fields and a json file representing consolidated statistics for a facebook asset.
        It will then create a list of jobs to be executed by the clickup job handler.
        """

        for fields, id in custom_fields.items():
            # split the fields
            split_fields = fields.lower().split()
            if (
                len(split_fields) < 2
                or split_fields[0] not in self.insights_field_mapping
                or split_fields[1] not in self.date_mapping
            ):
                continue

            # get the date preset
            date_preset = self.date_mapping[split_fields[1]]
            if date_preset not in asset_data.keys():
                continue
            # get the metric
            metric = self.insights_field_mapping[split_fields[0]]

            if asset_match_type == "name":
                asset = task["name"].lower()
            elif asset_match_type == "custom_field":
                asset = [
                    field["value"]
                    for field in task["custom_fields"]
                    if field["name"].lower() == custom_field_location.lower()
                ][0].lower()

            # If the asset is not in the date_preset data, set the value to 0.
            if asset not in asset_data[date_preset].keys():
                self.jobs.append((task["id"], id, 0))
                self.logger.debug(f"Created job for asset: {asset} {fields}: {id} value: 0")
                continue

            # get the value and handle key errors
            if isinstance(metric, tuple):
                value = asset_data[date_preset][asset][metric[0]].get(metric[1], 0)
            else:
                if metric.startswith("cost_per") and metric not in asset_data[date_preset][asset]:
                    value = None
                else:
                    value = asset_data[date_preset][asset][metric]
            # create the job
            if value:
                value = round(value, 2)
            self.jobs.append((task["id"], id, value))
            self.logger.debug(f"Created job for asset: {asset} {fields}: {id} value: {value}")
        return

    def create_insights_percent_job(self, task, custom_fields, asset_data):
        """
        Updates field with the name format as "{metric} %"
        """
        for fields, id in custom_fields.items():
            # split the fields => ["METRIC", "%"] and check if % sign in the name
            split_fields = fields.lower().split()
            if (
                len(split_fields) < 2
                or split_fields[0] not in self.insights_field_mapping
                or "%" not in split_fields[1]
            ):
                continue
            metric = split_fields[0]

            try:
                # get the value and handle key errors
                if isinstance(metric, tuple):
                    value = asset_data[metric[0]].get(metric[1], 0)
                else:
                    value = asset_data[metric]
            except KeyError:
                value = None
            # check if value of the field matches the value to insert
            if value in [field.get("value") for field in task["custom_fields"] if field["name"] == fields]:
                continue
            self.jobs.append((task["id"], id, value))
            self.logger.debug(f"Created job for asset: {task['name']} {fields}: {id} value: {value}")
        return

    def create_ads_with_issues_jobs(
        self, task, custom_fields, asset_data, asset_match_type="name", custom_field_location=None
    ):
        for fields, id in custom_fields.items():
            if fields.lower() != "issues":
                continue
            if asset_match_type == "name":
                asset = task["name"].lower()
            elif asset_match_type == "custom_field":
                asset = [
                    field["value"]
                    for field in task["custom_fields"]
                    if field["name"].lower() == custom_field_location.lower()
                ][0].lower()

            value = asset_data[asset]["issues"]
            self.jobs.append((task["id"], id, value))
            self.logger.debug(f"Created job for asset: {asset} {fields}: {id} value: {value}")
        return

    def create_ghl_jobs(self, task, custom_fields, asset_data):
        for fields, id in custom_fields.items():
            if fields.lower() not in ["appt 7", "appt fut"]:
                continue
            try:
                loc_id = [field["value"] for field in task["custom_fields"] if field["name"].lower() == "location id"][
                    0
                ]
            # key error or index error
            except KeyError or IndexError:
                continue
            value = asset_data[loc_id][fields.lower()]
            self.jobs.append((task["id"], id, value))
            self.logger.debug(f"Created job for asset: {loc_id} {fields}: {id} value: {value}")
        return

    def process_clickup_jobs(self, do_retry=True):
        """
        Executes a batch of jobs by updating custom fields in a task using a `ClickupClient` object.

        Args:
            clickup_client (object): A `ClickupClient` object with the class attributes `RATE_LIMIT_REMAINING` and `RATE_RESET`.
            jobs (list): A list of tuples, where each tuple contains the task, custom field id, and value for the custom field.

        Returns:
            dict: A dictionary with the following keys:
                - `failures` (list): A list of tuples, where each tuple represents a job that failed to execute.
        """
        retries = []
        batch = 0
        while len(self.jobs) > 0:
            status, _ = run_async_jobs(self.generate_jobs(), self.run_clickup_field_job)
            batch += 1
            retries.extend([job for job in status if isinstance(job, tuple)])
            self.logger.info(f"Batch {batch} complete. Remaining jobs: {len(self.jobs)}")
            self.wait_for_reset()
        # retries once
        if retries and do_retry:
            self.logger.info(f"Retrying {len(retries)} failed jobs.")
            self.jobs.extend(retries)
            self.process_clickup_jobs(do_retry=False)
        return

    def run_clickup_field_job(self, job):
        """
        Update the custom field for a task in Clickup.

        Args:
        - job (tuple): A tuple containing the following elements:
            - task id (str): A task's id.
            - custom_field_id (str): The id of the custom field to update.
            - value (str): The value to set the custom field to.

        Returns:
        - None: If the job is successful.
        - tuple: If the job fails, the tuple will be returned.

        """
        try:
            route = "task/" + job[0] + "/field/" + job[1]
            method = "POST"
            values = {"value": job[2]}
            _ = self.clickup_client.make_request(method=method, route=route, values=values)
            self.logger.debug(f"Updated task id: {job[0]} custom field id: {job[1]} with value: {job[2]}")
        # define the error that needs to be handled
        except Exception as e:
            # log the error, task id (job[0]), custom field id (job[1]), and value (job[2])
            self.logger.exception(
                f"Failed to update task id: {job[0]} custom field id: {job[1]} with value: {job[2]} error: {e}"
            )
            return job
        return

    def wait_for_reset(self):
        reset = float(self.clickup_client.RATE_RESET)
        sleep = reset - time.time()
        if sleep > 0:
            self.logger.info(f"Sleeping for {math.ceil(sleep)} seconds.")
            time.sleep(math.ceil(sleep))
        return

    def generate_jobs(self):
        self.clickup_client.refresh_rate_limit()
        rate = int(self.clickup_client.RATE_LIMIT_REMAINING)
        # pop the number of jobs equal to the rate limit and run them
        queued_jobs = []
        if rate > len(self.jobs):
            rate = len(self.jobs)
        for _ in range(rate):
            queued_jobs.append(self.jobs.pop())
        return queued_jobs
