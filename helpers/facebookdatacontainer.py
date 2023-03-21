from collections import defaultdict
from facebook_business.exceptions import FacebookRequestError
from helpers.helpers import run_async_jobs, extend_list_async, write_ads_to_csv
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from helpers.logging_config import BaseLogger
import time
import json


class FaceBookDataContainter(BaseLogger):
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

    def __init__(self):
        self.data = {
            "accounts": defaultdict(list),
            "failed_jobs": [],
            "reporting_data": defaultdict(list),
            "async_reports": defaultdict(list),
            "failed_account_ids": defaultdict(list),
            "rates": defaultdict(dict),
        }
        return super().__init__()

    def set_business_ad_accounts(self, business_id, fields=None):
        if fields is None:
            fields = [AdAccount.Field.account_status, AdAccount.Field.name]
        if AdAccount.Field.account_status not in fields:
            fields.append(AdAccount.Field.account_status)
        accounts = self.retrieve_ad_accounts(business_id, fields=fields)
        for account in accounts:
            account_status = str(account["account_status"])
            self.data["accounts"][self.ad_account_status_mapping[account_status]].append(account)
            self.logger.debug(
                f"Added account {account[AdAccount.Field.id]} to business {business_id} with status {account_status}"
            )
        self.logger.info(f"{len(accounts)} accounts found for business {business_id}")
        return self

    def retrieve_ad_accounts(self, business_id, fields=None):
        if fields is None:
            fields = ["account_status"]
        if "account_status" not in fields and "name" not in fields:
            fields.append("name")
            fields.append("account_status")
        business = Business(business_id).api_get(fields=["id", "name"])
        all = []
        seen = set()
        for agency in [business, *self.call_method(business, "get_agencies", ["id", "name"])]:
            # get all ad accounts for the agency
            owned = self.call_method(agency, "get_owned_ad_accounts", fields=fields)
            clients = self.call_method(agency, "get_client_ad_accounts", fields=fields)
            accounts = [*owned, *clients]
            all.extend([account for account in accounts if account["id"] not in seen and not seen.add(account["id"])])
        return all

    def retrieve_facebook_objects_insights(self, facebook_objects, fields, params):
        date_preset = params["date_preset"]
        results, failed_jobs = run_async_jobs(facebook_objects, self.retrieve_insights, fields, params)
        self.data["failed_jobs"].extend(failed_jobs)
        accounts = [obj for obj in results if obj.__class__.__name__ == "AdAccount" and not results.remove(obj)]
        if accounts:
            finished_reports_or_accounts, failed_jobs = run_async_jobs(
                accounts, self.process_async_report, fields, params
            )
            self.logger.info(f"Finished processing async reports ({date_preset})")
            self.data["failed_jobs"].extend(failed_jobs)
            accounts = [
                obj
                for obj in finished_reports_or_accounts
                if obj.__class__.__name__ == "AdAccount" and not finished_reports_or_accounts.remove(obj)
            ]
            results.extend(finished_reports_or_accounts)
            self.data["failed_account_ids"][date_preset].extend(accounts)
        run_async_jobs(results, extend_list_async, self.data["reporting_data"][date_preset])
        self.logger.info(f"Finished retrieving insights ({date_preset})")
        return self

    def retrieve_insights(self, facebook_object, fields, params):
        # retrieve insights for a single ad account and return the results,
        # if there is an error.body() and error["error"]["code"] is 1 or 100, then an async report is generated, processed, and the results are returned
        # check rates

        try:
            cursor = self.call_method(facebook_object, "get_insights", fields, params)
            return cursor
        except FacebookRequestError as e:
            error = e.body()["error"]
            if error.get("code", None) in [1, 100]:  # async needed
                return facebook_object
            self.logger.exception(e)
            return

    def process_async_report(self, facebook_object, fields, params, max_attempts=5, initial_delay=60):
        # creates an async report, waits for it to finish, and returns the reports. Retries up to 5 times with an exponential backoff.
        attempts = 0
        first_run = True
        while first_run or report[AdReportRun.Field.async_status] != "Job Completed" and attempts <= max_attempts:
            if first_run:
                first_run = False
            report = self.call_method(facebook_object, "get_insights_async", fields, params)
            report = self.wait_for_job(report)
            attempts += 1
            if report[AdReportRun.Field.async_status] == "Job Completed":
                return report
            if attempts > max_attempts:
                return facebook_object
            self.logger.info(
                f"Async report for facebook object id: {facebook_object['id']} failed with a async_status of {report[AdReportRun.Field.async_status]}. Retrying in {initial_delay} seconds. Attempt {attempts} of {max_attempts}."
            )
            time.sleep(initial_delay)
            initial_delay *= 2
        return

    def wait_for_job(self, report, timeout=600):
        """
        Processes async reports and returns the results. There is timeout of 10 minutes.
        """
        end_time = time.time() + timeout
        report = self.call_method(report, "api_get")
        while (
            report[AdReportRun.Field.async_status] in ["Job Running", "Job Not Started", "Job Started"]
            and time.time() < end_time
        ):
            report = self.call_method(report, "api_get")
            time.sleep(10)
        return report

    def extract_business_use_case_usage(self, headers):
        if "x-business-use-case-usage" not in headers:
            return
        usage_data = json.loads(headers["x-business-use-case-usage"])
        for account_id, data in usage_data.items():
            type = data[0]["type"]
            call_count = data[0]["call_count"]
            total_cputime = data[0]["total_cputime"]
            estimated_time_to_regain_access = data[0]["estimated_time_to_regain_access"]
            self.data["rates"][account_id] = {
                "type": type,
                "call_count": call_count,
                "total_cputime": total_cputime,
                "estimated_time_to_regain_access": estimated_time_to_regain_access,
            }
        return True

    def call_method(self, object, method_name, *args, **kwargs):
        """
        Calls a method on an object and returns the results.
        """
        method = getattr(object, method_name)
        result = method(*args, **kwargs)
        # TODO - this is a hack to get the business use case usage data. Need to find a better way to do this.
        if method_name == "get_insights":
            self.extract_business_use_case_usage(result.headers())

        def build_string(result):
            string = f"Called method: {method_name} on object: {object.__class__.__name__}"
            if result.__class__.__name__ == "Cursor":
                string += f" with Result: Cursor"
                return string
            else:
                string += f" with Result: {result.__class__.__name__}"
                if not isinstance(result, dict):
                    return string
                for k, v in result.items():
                    string += f" {k}: {v},"
            return string

        self.logger.debug(build_string(result))
        return result

    def export_all_reporting_data(self):
        """
        Exports all data to a csv file.
        """
        for date_preset, data in self.data["reporting_data"].items():
            self.logger.info(f"Writing {date_preset} reporting data to csv")
            write_ads_to_csv([d.export_all_data() for d in data], f"uploads/{date_preset}.csv")
        return self
