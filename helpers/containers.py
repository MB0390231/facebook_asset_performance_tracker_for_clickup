from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from handlers.logging_config import get_logger
import time
import math
from handlers import helpers
import threading

logger = get_logger(__name__)


class FacebookDataContainer:
    account_limits = {"x-business-use-case-usage": {}, "x-ad-account-usage": {}, "x-app-usage": {}}

    def __init__(self, facebook_client, DEFAULT_INSIGHTS_FIELDS, DEFAULT_INSIGHTS_PARAMS):
        self.DEFAULT_INSIGHTS_FIELDS = DEFAULT_INSIGHTS_FIELDS
        self.DEFAULT_INSIGHTS_PARAMS = DEFAULT_INSIGHTS_PARAMS
        self.api = facebook_client
        self.ads_data_lock = threading.Lock()
        self.date_presets = []
        self.reports = {}
        self.failed_reports = {}
        self.failed_requests = {}
        self.failed_futures = {}
        self.ads_data = {}
        self.no_data = {}
        self.ads_with_issues = []
        return

    def set_business_ad_accounts(self, business_id, fields=None):
        logger.info("Setting ad business accounts...")
        if fields is None:
            fields = ["account_status", "name", "id"]
        all = [
            *Business(business_id).get_owned_ad_accounts(fields=fields),
            *Business(business_id).get_client_ad_accounts(fields=fields),
        ]
        for account in all:
            account_status = account["account_status"]
            if account_status == 1:
                if not hasattr(self, "active_accounts"):
                    self.active_accounts = []
                self.active_accounts.append(account)
            elif account_status == 2:
                if not hasattr(self, "disabled_accounts"):
                    self.disabled_accounts = []
                self.disabled_accounts.append(account)
            elif account_status == 3:
                if not hasattr(self, "unsettled_accounts"):
                    self.unsettled_accounts = []
                self.unsettled_accounts.append(account)
            elif account_status == 7:
                if not hasattr(self, "pending_risk_review_accounts"):
                    self.pending_risk_review_accounts = []
                self.pending_risk_review_accounts.append(account)
            elif account_status == 8:
                if not hasattr(self, "pending_settlement_accounts"):
                    self.pending_settlement_accounts = []
                self.pending_settlement_accounts.append(account)
        logger.info("Ad business accounts set.")
        return

    def generate_data_for_date_presets(self, accounts, date_presets):
        """
        Generates data for a list of date presets.
        """
        for dates in date_presets:
            self.create_reports(accounts, dates)
        self.process_all_reports()
        for dates in date_presets:
            self.process_report_result(dates)
        return

    def create_reports(self, accounts, date_preset):
        logger.debug(f"Creating reports for date preset: {date_preset}")
        self.add_date_preset_default(date_preset)
        fields = self.DEFAULT_INSIGHTS_FIELDS.copy()
        params = self.DEFAULT_INSIGHTS_PARAMS.copy()
        params["date_preset"] = date_preset
        success, failure = helpers.run_async_jobs(accounts, self.create_report, fields, params)
        self.reports[date_preset] = success
        self.failed_requests[date_preset] = failure
        logger.debug(f"Reports created for date preset: {date_preset}")
        logger.debug(f"     Number of successful reports created: {len(success)}")
        logger.debug(f"     Number of failed requests: {len(failure)}")
        return

    def add_date_preset_default(self, date_preset):
        """
        Adds date preset to instance attributes that are dictinaries so I can assume the date persets are there later
        """
        if type(date_preset) is not str:
            raise TypeError("date_preset must be a string")
        if date_preset not in self.date_presets:
            self.date_presets.append(date_preset)
        for attr in self.__dict__.values():
            if type(attr) is dict and date_preset not in attr.keys():
                attr[date_preset] = []
        return

    def create_report(self, account, fields, params):
        """
        Creates a Facebook AdReportRun report for a given account.

        Parameters:
            - account (AdAccount): A Facebook AdAccount object representing the account to create a report for.
            - fields (list): A list of fields to include in the report.
            - params (dict): A dictionary of parameters to include in the report.

        Returns:
            A Facebook AdReportRun object representing the created report.

        The function creates a report for the given account and returns the report object. If the report fails to create, the function returns None.
        """
        try:
            job = account.get_insights_async(fields=fields, params=params)
        except Exception as e:
            logger.debug(f"Failed to create report for account: {account['id']}")
            logger.warning(
                f"{id}: message : {e.body()['error']['message']} : code : {e.body()['error']['code']} : subcode : {e.body()['error']['error_subcode']} : type : {e.body()['error']['error_user_title']} : description : {e.body()['error']['error_user_msg']}"
            )
        return job

    def process_all_reports(self):
        """
        processes reports for every date preset.
        """
        for dates in self.date_presets:
            self.process_date_presets_reports(dates)
        return

    def process_date_presets_reports(self, date_preset):
        """
        processes reports for a given date preset.
        """
        # Reports should automatically update in self.reports
        reports = self.reports[date_preset]
        if reports:
            logger.debug(f"Processing reports for {date_preset}...")
            _, failure = helpers.run_async_jobs(reports, self.wait_for_job, date_preset)
            self.failed_futures[date_preset].extend(failure)
            logger.debug("Reports processed.")
            logger.debug(f"     Number of failed reports: {len(failure)}")
        else:
            logger.debug("No reports to process.")
        return

    def wait_for_job(self, report, date_preset):
        """
        Waits for a Facebook AdReportRun report to complete and returns the results.

        Parameters:
            - report (AdReportRun): A Facebook AdReportRun object representing the report to wait for.

        Returns:
            A Facebook AdReportRun object representing the completed report, including the results.

        The function retrieves the status of the report every 10 seconds until the report is marked as complete or failed. If the report fails, the current status of the report is returned. If the report is successful, the function retrieves the final result of the report and returns it.
        """
        try:
            report = report.api_get()
        except Exception as e:
            id = e.request_context()["path"].split("/")[-2]
            logger.debug(f"Failed to retrieve report: {id}")
            logger.warning(
                f"{id}: message : {e.body()['error']['message']} : code : {e.body()['error']['code']} : subcode : {e.body()['error']['error_subcode']} : type : {e.body()['error']['error_user_title']} : description : {e.body()['error']['error_user_msg']}"
            )
            logger.debug("Retrying in 5 seconds...")
            time.sleep(5)
            try:
                report = report.api_get()
                logger.log("Retried successfully.")
            except:
                self.failed_reports[date_preset].append(report)
                logger.log("Retried unsuccessfully.")
                return
        while report[AdReportRun.Field.async_status] in ("Job Not Started", "Job Running", "Job Started"):
            time.sleep(10)
            report = report.api_get()
        if report[AdReportRun.Field.async_status] != "Job Completed":
            self.reports[date_preset].remove(report)
            self.failed_reports[date_preset].append(report)
        return

    def process_all_reports_results(self):
        """
        Processes the results of all reports.
        """
        for dates in self.date_presets:
            self.process_report_result(dates)
        return

    def process_report_result(self, date_preset):
        """
        Processes the results of the reports for a given date preset.
        """
        reports = self.reports[date_preset]
        if reports:
            logger.debug(f"Processing report results for date preset: {date_preset}")
            success, failure = helpers.run_async_jobs(reports, self.add_facebook_report_results, date_preset)
            self.failed_futures[date_preset].extend(failure)
            logger.debug(f"Report results processed for date preset: {date_preset}")
            logger.debug(f"     Number of successful report results: {len(success)}")
            logger.debug(f"     Number of failed report results: {len(failure)}")
        else:
            logger.debug("No report results to process.")
        return

    def add_facebook_report_results(self, report, date_preset):
        """
        adds reports results to self.ads_data[date_preset]
        """
        cursor = report.get_result(params={"limit": 500})
        if len(cursor) > 0:
            with self.ads_data_lock:
                self.ads_data[date_preset].extend(cursor)
        else:
            self.no_data[date_preset].append(report[AdReportRun.Field.account_id])
        return

    def retry_failed_reports_non_async(self):
        """
        Runs through every ad report for every date preset and retrieves insights non async to ensure every account is added
        """
        for dates in self.date_presets:
            account = self.failed_reports[dates]
            if not account:
                continue
            retry = [
                AdAccount(f"act_{report['account_id']}").api_get(fields=["account_id"])
                for report in self.failed_reports[dates]
            ]
            logger.debug(f"Retrying {len(retry)} reports for date_preset {dates}")
            data = []
            fields = self.DEFAULT_INSIGHTS_FIELDS.copy()
            params = self.DEFAULT_INSIGHTS_PARAMS.copy()
            params["date_preset"] = dates
            for account in retry:
                cursor = account.get_insights(fields=fields, params=params)
                logger.debug(f"    Successfully retrieved insights for account: {account['account_id']}")
                data.extend(cursor)
            self.ads_data[dates].extend(data)

    def process_ads_with_issues(self, accounts, fields, params):
        """
        Retrieve ads with issues and returns a list of ads with issues and a configured status of active. Ads with issues that were paused by the user are not returned.

        """
        logger.debug(f"Processing ads with issues")
        success, failure = helpers.run_async_jobs(accounts, self.request_issues, fields, params)
        logger.debug(f"     Number of Successful Requests: {len(success)}")
        logger.debug(f"     Number of Failed Requests: {len(failure)}")
        self.ads_with_issues = [ad for lists in success for ad in lists if ad["configured_status"] == "ACTIVE"]
        if "ads_with_issues" not in self.failed_futures.keys():
            self.failed_futures["ads_with_issues"] = []
        self.failed_futures["ads_with_issues"].extend(failure)
        return

    def request_issues(self, account, fields, params):
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

        issues = account.get_ads(fields=fields, params=params)
        # TODO: update rate limits
        # self.update_account_rates(issues.headers())

        return issues

    # #TODO:
    # def update_account_rates(self,headers):
    #     """
    #     Updates the headers for accounts insights
    #     {
    #     "account_id": {
    #         "ads_insights": {
    #             "call_count": 0,
    #             "total_cputime": 0,
    #             "total_time": 0,
    #             "estimated_time_to_regain_access": 0,
    #         },
    #         "ads_management": {
    #             "call_count": 0,
    #             "total_cputime": 0,
    #             "total_time": 0,
    #             "estimated_time_to_regain_access": 0,
    #         },
    #     }

    #     """
    #     #Try json.load(headers['x-business-use-case-usage'])
    #     headers = json.load(headers)
    #     for keys in self.account_limits.keys():
    #         try:
    #             self.account_limits[keys]
    #         except:
    #     pass

    # #TODO:
    # def log_report_errors(facebook_data):
    #     """
    #     Logs errors for reports that were not successful.

    #     Parameters:
    #         - reports (list): A list of reports that were not successful.
    #     """
    #     for date_presets in facebook_data["data"].keys():
    #         if len(facebook_data["data"][date_presets]["unsuccessful_request_futures"]) > 0:
    #             print(f"Unsuccessful requests for {date_presets}:")
    #             for future in facebook_data["data"][date_presets]["unsuccessful_request_futures"]:
    #                 print(f"    {future.exception()}")
    #             print("")
    #         if len(facebook_data["data"][date_presets]["unsuccessful_reports"]) > 0:
    #             print(f"Unsuccessful reports for {date_presets}:")
    #             for report in facebook_data["data"][date_presets]["unsuccessful_reports"]:
    #                 print(f"    {report}")
    #             print("")


class FacebookAssetGroups:
    """
    An AssetGroup is a collection of assets
    An asset is a regex pattern meant to be found in an objs target_key
    """

    def __init__(self, data_container) -> None:
        self.data_container = data_container
        self.reports = {}
        pass

    def add_asset_group(self, target_key, regex_pattern, asset_group_name):
        """
        Creates asset_group groups where an asset_group is a regex pattern meant to be found in an objs target_key
        If the asset group is already created, it refreshes the asset groups data.
        automatically organizes facebook data from all date presets
        results in the following dictionary
        {
            "date_preset":{
                "asset_group":data,
            },
            "date_preset":{
                "asset_group":data,
            }
        }
        """
        # TODO:
        # set attribute for which ever data set (ex. retailer data or creative data)
        self.create_asset_group(asset_group_name)
        logger.debug(f"Processing asset group {asset_group_name}")
        group = self.__getattribute__(asset_group_name)
        for preset in self.data_container.date_presets:
            for obj in self.data_container.ads_data[preset]:
                extracted = helpers.extract_regex_expression(obj[target_key], regex_pattern)
                if extracted:
                    asset_id = extracted.lower()
                    if asset_id not in group[preset].keys():
                        group[preset][asset_id] = [obj]
                    else:
                        group[preset][asset_id].append(obj)
        logger.debug(f"Processed asset group {asset_group_name}")
        self.generate_asset_report(asset_group_name)
        return group

    def create_asset_group(self, asset_group):
        """
        Sets or resets the asset group to be used for the rest of the class
        """
        # set attr if not already set
        if not hasattr(self, f"{asset_group}"):
            self.__setattr__(f"{asset_group}", {})
            logger.debug(f"Asset group {asset_group} created")
        else:
            self.__setattr__(f"{asset_group}", {})
            logger.debug(f"Asset group {asset_group} reset")
        for dates in self.data_container.date_presets:
            self.__getattribute__(asset_group)[dates] = {}
        return

    def generate_asset_report(self, asset_group):
        """
        Generates a report for an asset group for every date_preset that has data. If the asset group has not been generated yet, it raises and attribute error
        """
        if not hasattr(self, f"{asset_group}"):
            raise AttributeError(
                f"{self.__name__} does not have asset_group: {asset_group}.\n"
                f"Please generate asset group first using {self.__name__}.facebook_data_organized_by_regex()"
            )

        report = {}
        group = self.__getattribute__(asset_group)
        for dates in self.data_container.date_presets:
            report[dates] = {}
            for keys, value in group[dates].items():
                report[dates][keys] = self.consolidate_objects_stats(value)
        self.reports[asset_group] = report
        logger.debug(f"Generated report for asset group {asset_group}")
        return

    # TODO: use a mapping to map the keys to the correct function. EX "spend": total(), "CTR": weighted_average()
    def consolidate_objects_stats(self, list_of_objects):
        """
        Creates a dictionary of consolidated stats for a list of objects
        It is meant to run through a list of objects relating to a single asset group
        """
        consolidation = {
            "count": len(list_of_objects),
            "actions": {},
        }
        # values and weights for weighted average with values being the metric and weights being the spend
        ctr_values = []
        weights = []
        cpm_values = []
        for obj in list_of_objects:
            spend = float(obj["spend"])
            consolidation["spend"] = consolidation.get("spend", 0) + spend
            # actions (leads, purchases, ect.)
            for actions in obj.get("actions", []):
                consolidation["actions"][actions["action_type"]] = consolidation["actions"].get(
                    f"{actions['action_type']}", 0
                ) + float(actions["value"])

            # add the values and weights for weighted average
            weights.append(spend)
            ctr_values.append(float(obj.get("inline_link_click_ctr", 0)))
            cpm_values.append(float(obj.get("cpm", 0)))

        # calculate cost per actions
        for actions in consolidation.get("actions", []):
            consolidation[f"cost_per_{actions}"] = consolidation["spend"] / consolidation["actions"][actions]

        # calculate averages for CPC, CPM, and CTR
        consolidation["cpm"] = helpers.weighted_average(cpm_values, weights)
        consolidation["ctr"] = helpers.weighted_average(ctr_values, weights)

        return consolidation


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
        "spend": "spend",
        "leads": ("actions", "lead"),
        "purchases": ("actions", "purchase"),
        "cpl": "cost_per_lead",
        "cpp": "cost_per_purchase",
        "cpc": "cost_per_link_click",
        "cpm": "cpm",
        "ctr": "ctr",
        "count": "count",
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
        self.failed_futures = []

    def create_insights_jobs(self, task, custom_fields, asset_data):
        """
        This method will take in a dictionary of custom fields and a json file representing consolidated statistics for a facebook asset.
        It will then create a list of jobs to be executed by the clickup job handler.
        """
        jobs = []

        # I need to split up the custom fields, use the numbers to determine which date preset of a report to use, and use the letters to determine which metric to use.
        for fields, id in custom_fields.items():
            # split the fields
            split_fields = fields.lower().split()
            if (
                len(split_fields) < 2
                or split_fields[0] not in self.insights_field_mapping
                or split_fields[1] not in self.date_mapping
            ):
                # TODO: set value to 0
                continue

            # get the date preset
            date_preset = self.date_mapping[split_fields[1]]

            # get the metric
            metric = self.insights_field_mapping[split_fields[0]]

            asset = task["name"].lower()

            # If the asset is not in the date_preset data, set the value to 0.
            if asset not in asset_data[date_preset].keys():
                self.jobs.append((task["id"], id, 0))
                logger.debug(f"Created job for asset: {asset}, {fields}: {id} value: 0")
                continue

            # spend will be the default value if the custom_field begins with "cp" and the metric is not found.
            spend = asset_data[date_preset][asset]["spend"]

            # get the value and handle key errors
            if isinstance(metric, tuple):
                value = asset_data[date_preset][asset][metric[0]].get(metric[1], 0)
                # value = asset_data[date_preset][asset][metric[0]].[metric[1]]
            else:
                if metric.startswith("cost_per") and metric not in asset_data[date_preset][asset]:
                    value = spend
                else:
                    value = asset_data[date_preset][asset][metric]
            # create the job
            round_value = round(value, 2)
            self.jobs.append((task["id"], id, round_value))
            logger.debug(f"Created job for asset: {asset}, {fields}: {id} value: {round_value}")
        return

    def process_clickup_jobs(self):
        """
        Executes a batch of jobs by updating custom fields in a task using a `ClickupClient` object.
        Automatically chooses the amount of jobs to run based on the rate limit remaining and the rate limit reset time.
        """
        complete = False

        pass

    def process_clickup_jobs(self):
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
        failures = []
        retries = []
        batch = 0
        while not complete:
            self.clickup_client.refresh_rate_limit()
            rate = int(self.clickup_client.RATE_LIMIT_REMAINING)
            reset = float(self.clickup_client.RATE_RESET)
            # pop the number of jobs equal to the rate limit and run them
            queued_jobs = []
            if rate > len(self.jobs):
                rate = len(self.jobs)
                complete = True
            for _ in range(rate):
                queued_jobs.append(self.jobs.pop())
            retry, failure = helpers.run_async_jobs(queued_jobs, self.run_clickup_field_job)
            batch += 1
            logger.debug(f"Batch {batch} complete.")
            if failure:
                logger.debug("      Failed jobs: " + str(len(failure)))
            retries.extend([job for job in retry if isinstance(job, tuple)])
            failures.extend(failure)
            sleep = reset - time.time()
            if sleep > 0 and not complete:
                logger.debug(f"Sleeping for {math.ceil(sleep)} seconds.")
                time.sleep(math.ceil(sleep))
        if retries:
            logger.debug(f"Failed jobs: {len(retries)}. Re-added into the queue.")
        self.jobs = retries
        self.failed_futures.extend(failures)
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
            response = self.clickup_client.make_request(method=method, route=route, values=values)
        except Exception as e:
            # log the error, task id (job[0]), custom field id (job[1]), and value (job[2])
            logger.debug(
                f"Failed to update task id {job[0]} custom field id {job[1]} with value {job[2]} error message {e}"
            )
            return job
        return
