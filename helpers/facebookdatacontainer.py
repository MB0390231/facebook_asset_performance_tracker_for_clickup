from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from helpers.logging_config import BaseLogger
import time
from helpers import helpers
import threading


class FacebookDataContainer(BaseLogger):
    account_limits = {"x-business-use-case-usage": {}, "x-ad-account-usage": {}, "x-app-usage": {}}
    ad_account_status_mapping = {
        "1": "ACTIVE",
        "2": "DISABLED",
        "3": "UNSETTLED",
        "7": "PENDING_RISK_REVIEW",
        "8": "PENDING_SETTLEMENT",
        "9": "IN_GRACE_PERIOD",
        "100": "PENDING_CLOSURE",
        "101": "CLOSED",
        "201": "ANY_ACTIVE",
        "202": "ANY_CLOSED",
    }
    # get logger with the name as the class name

    def __init__(self, facebook_client):
        self.api = facebook_client
        self.thread_lock = threading.Lock()
        self.date_presets = []
        self.async_reports = {}
        self.failed_async_reports = {}
        self.failed_futures = {}
        self.insights_data = {}
        self.accounts = {}
        return super().__init__()

    def set_business_ad_accounts(self, business_id, fields=None):
        if fields is None:
            fields = ["account_status"]
        if "account_status" not in fields:
            fields.append("account_status")
        accounts = self.retrieve_ad_accounts(business_id, fields=fields)
        for account in accounts:
            account_status = str(account["account_status"])
            if self.ad_account_status_mapping[account_status] not in self.accounts.keys():
                self.accounts[self.ad_account_status_mapping[account_status]] = []
            self.accounts[self.ad_account_status_mapping[account_status]].append(account)
            self.logger.debug(
                f"Added account {account[AdAccount.Field.id]} to business {business_id} with status {account_status}"
            )
        self.logger.info(f"{len(accounts)} accounts found for business {business_id}")
        return

    def retrieve_ad_accounts(self, business_id, fields=None):
        if fields is None:
            fields = ["account_status"]
        if "account_status" not in fields and "name" not in fields:
            fields.append("name")
            fields.append("account_status")
        business = Business(business_id).api_get(fields=["id", "name"])
        all = []
        seen = set()
        for agency in [business, *self.call_method(business, "get_agencies", fields=["id", "name"])]:
            self.logger.debug(f"Agency {agency['name']} found for business {business_id}")
            # get all ad accounts for the agency
            accounts = [
                *self.call_method(agency, "get_owned_ad_accounts", fields=fields),
                *self.call_method(agency, "get_client_ad_accounts", fields=fields),
            ]
            for account in accounts:
                if account["id"] not in seen:
                    all.append(account)
                    seen.add(account["id"])
        return all

    def generate_data_for_batch_date_presets(self, accounts, date_presets, fields, params):
        """
        Generates data for a list of date presets.
        """

        for dates in date_presets:
            self._add_date_preset_default(dates)
            self.create_reports(accounts, dates, fields, params)
        self.process_all_reports()
        for dates in date_presets:
            self.process_report_result(dates)
        for dates in date_presets:
            self.retry_failed_async_reports(dates, fields, params)
        return

    def generate_data_for_date_preset(self, accounts, date_preset, fields, params):
        """
        Generates data for a single date preset.
        """
        self._add_date_preset_default(date_preset)
        self.create_reports(accounts, date_preset, fields, params)
        self.process_all_reports()
        self.process_report_result(date_preset)
        self.retry_failed_async_reports(fields, params)
        return

    def create_reports(self, accounts, date_preset, fields, params):
        params["date_preset"] = date_preset
        success, failure = helpers.run_async_jobs(accounts, self.create_report, fields, params)
        self.async_reports[date_preset] = success
        self.failed_futures[date_preset] = failure
        self.logger.info(
            f"Created {len(accounts)} reports ({date_preset}). Successful: {len(success)}. Failed: {len(failure)}"
        )
        return

    def _add_date_preset_default(self, date_preset):
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
        # Reports should automatically update in self.async_reports
        reports = self.async_reports[date_preset]
        if reports:
            _, failure = helpers.run_async_jobs(reports, self.wait_for_job, date_preset)
            self.failed_futures[date_preset].extend(failure)
            self.logger.info(f"Reports finished for {date_preset}. Successful: {len(_)}. Failed: {len(failure)}")
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
        reports = [
            report
            for report in self.async_reports[date_preset]
            if report[AdReportRun.Field.async_status] == "Job Completed"
        ]
        success, failure = helpers.run_async_jobs(reports, self.add_facebook_report_results, date_preset)
        self.failed_futures[date_preset].extend(failure)
        self.logger.info(f"Results processed for {date_preset}. Successful: {len(success)}. Failed: {len(failure)}")
        return

    def retry_failed_async_reports(self, date_preset, fields, params):
        """
        Runs through every ad report for every date preset and retrieves insights non async to ensure every account is added
        """
        if not self.failed_async_reports[date_preset]:
            self.logger.info(f"No failed async reports for {date_preset}")
            return
        retry = [
            AdAccount(f"act_{report['account_id']}").api_get(fields=["account_id"])
            for report in self.failed_async_reports[date_preset]
        ]
        data = []
        params["date_preset"] = date_preset
        # TODO: Follow the standard of using async here
        for account in retry:
            cursor = account.get_insights(fields=fields, params=params)
            self.logger.debug(
                f"Successfully retried insights retrieval for account: {account['account_id']} for date preset: {date_preset}"
            )
            data.extend(cursor)
        self.insights_data[date_preset].extend(data)
        self.logger.info(f"Successfully retried failed facebook reports for {date_preset}")
        return

    def retrieve_ads(self, accounts, fields, params):
        """
        Retrieve ads for a given account and returns a list of ads.
        """
        success, failure = helpers.run_async_jobs(accounts, self.request_ads, fields, params)
        self.logger.info(
            f"Retrieved ads for {len(accounts)} accounts. Number of Successful Requests: {len(success)}. Number of Failed Requests: {len(failure)}"
        )
        self.ads = [ad for lists in success for ad in lists]
        if "ads" not in self.failed_futures.keys():
            self.failed_futures["ads"] = []
        self.failed_futures["ads"].extend(failure)
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
        job = account.get_insights_async(fields=fields, params=params)
        self.logger.debug(f"Created report for account: {account['id']}")
        return job

    def wait_for_job(self, report, date_preset):
        """
        Waits for a Facebook AdReportRun report to complete and returns the results.

        Parameters:
            - report (AdReportRun): A Facebook AdReportRun object representing the report to wait for.

        Returns:
            A Facebook AdReportRun object representing the completed report, including the results.

        The function retrieves the status of the report every 10 seconds until the report is marked as complete or failed. If the report fails, the current status of the report is returned. If the report is successful, the function retrieves the final result of the report and returns it.
        """
        # TODO: reduce risk for async race conditions
        report = report.api_get()
        while report[AdReportRun.Field.async_status] in ("Job Not Started", "Job Running", "Job Started"):
            time.sleep(10)
            report = report.api_get()
        if report[AdReportRun.Field.async_status] != "Job Completed":
            self.failed_async_reports[date_preset].append(report)
        return

    def add_facebook_report_results(self, report, date_preset):
        """
        adds reports results to self.insights_data[date_preset]
        """
        cursor = report.get_result(params={"limit": 250})
        self.logger.debug(f"Successfully retrieved insights for account: {report[AdReportRun.Field.account_id]}")
        if len(cursor) > 0:
            with self.thread_lock:
                self.insights_data[date_preset].extend([obj.export_all_data() for obj in cursor])
        return

    def request_ads(self, account, fields, params):
        """
        Retrieve ads for a given account and returns a list of ads.
        """
        cursor = account.get_ads(fields=fields, params=params)
        self.logger.debug(f"Successfully retrieved ads for account: {account[AdAccount.Field.id]}")
        return cursor

    def call_method(self, object, method_name, *args, **kwargs):
        """
        Calls a method on an object and returns the results.
        """
        try:
            method = getattr(object, method_name)
            result = method(*args, **kwargs)
            self.logger.debug(
                f"Successfully called method: {method_name} on object: {object.__class__.__name__} with id: {object.get('id',None)} with name: {object.get('name',None)}"
            )
            return result
        except Exception as e:
            self.logger.error(f"Failed to call method: {method_name} on object: {object.__class__.__name__}")
            self.logger.error(e)
            return None

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
