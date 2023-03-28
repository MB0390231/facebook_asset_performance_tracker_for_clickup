from collections import defaultdict
from facebook_business.exceptions import FacebookRequestError
from helpers.helpers import (
    run_async_jobs,
    extend_list_async,
    write_dicts_to_csv,
    generate_reporting_row,
    generate_rejected_ad_row,
)
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from helpers.logging_config import BaseLogger
from time import sleep, time
import json


class FaceBookDataContainer(BaseLogger):
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
        self.accounts = defaultdict(list)
        self.failed_jobs = []
        self.reporting_data = defaultdict(list)
        self.ads = []
        self.failed_account_ids = defaultdict(list)
        self.rates = defaultdict(dict)
        return super().__init__()

    def set_business_ad_accounts(self, business_id, fields=None):
        if fields is None:
            fields = [AdAccount.Field.account_status, AdAccount.Field.name]
        if AdAccount.Field.account_status not in fields:
            fields.append(AdAccount.Field.account_status)
        accounts = self.retrieve_ad_accounts(business_id, fields=fields)
        for account in accounts:
            account_status = str(account["account_status"])
            self.accounts[self.ad_account_status_mapping[account_status]].append(account)
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

    def retreve_ads(self, accounts, fields, params):
        ads, failed_futures = run_async_jobs(accounts, self.call_method, "get_ads", fields, params)
        self.logger.info(f"Retrieved {len(ads)} ads")
        self.ads.extend([ad for l in ads for ad in l])
        self.failed_jobs.extend(failed_futures)
        return self

    # TODO: refactor this method to be more readable
    def retrieve_facebook_objects_insights(self, facebook_objects, fields, params):
        date_preset = params["date_preset"]
        results, failed_jobs = run_async_jobs(facebook_objects, self.retrieve_insights, fields, params)
        self.failed_jobs.extend(failed_jobs)
        accounts = [obj for obj in results if obj.__class__.__name__ == "AdAccount" and not results.remove(obj)]
        if accounts:
            finished_reports_or_accounts, failed_jobs = run_async_jobs(
                accounts, self.process_async_report, fields, params
            )
            self.logger.info(f"Finished processing async reports ({date_preset})")
            self.failed_jobs.extend(failed_jobs)
            accounts = [
                obj
                for obj in finished_reports_or_accounts
                if obj.__class__.__name__ == "AdAccount" and not finished_reports_or_accounts.remove(obj)
            ]
            results.extend(finished_reports_or_accounts)
            self.failed_account_ids[date_preset].extend(accounts)
        run_async_jobs(results, extend_list_async, self.reporting_data[date_preset])
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
            sleep(initial_delay)
            initial_delay *= 2
        return

    def wait_for_job(self, report, timeout=600):
        """
        Processes async reports and returns the results. There is timeout of 10 minutes.
        """
        end_time = time() + timeout
        report = self.call_method(report, "api_get")
        while (
            report[AdReportRun.Field.async_status] in ["Job Running", "Job Not Started", "Job Started"]
            and time() < end_time
        ):
            report = self.call_method(report, "api_get")
            sleep(10)
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
            self.rates[account_id] = {
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
                for k, v in result.items():
                    string += f" {k}: {v},"
            return string

        self.logger.debug(build_string(result))
        return result

    def export_all_reporting_data_csv(self):
        """
        Exports all data to a csv file.
        """

        for date_preset, data in self.reporting_data.items():
            fieldnames = {key for d in data for key in d.keys()}
            fieldnames |= {"lead", "purchase"}
            self.logger.info(f"Writing {date_preset} reporting data to csv")
            write_dicts_to_csv(
                self.reporting_data[date_preset], fieldnames, generate_reporting_row, f"data/{date_preset}.csv"
            )
        if self.ads:
            fieldnames = {key for ad in self.ads for key in ad.keys()}
            fieldnames |= {"retailer_id", "creative_id", "copy_id"}
            write_dicts_to_csv(self.ads, fieldnames, generate_rejected_ad_row, "data/ads_with_issues.csv")
        return self

    def export_all_reporting_data_json(self):
        """
        exports all data to a json file
        """
        for date_preset, data in self.reporting_data.items():
            self.logger.info(f"Writing {date_preset} reporting data to csv")
            with open(f"data/{date_preset}.json", "w") as file:
                json.dump(data, file, indent=4, default=lambda x: x.export_all_data())

        return self