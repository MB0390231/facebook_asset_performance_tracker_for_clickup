from collections import defaultdict
from facebook_business.exceptions import FacebookRequestError
from helpers.helpers import (
    run_async_jobs,
    extend_list_async,
    write_dicts_to_csv,
    generate_reporting_row,
    generate_rejected_ad_row,
)
from gohighlevel_python_sdk.exceptions import GHLRequestError
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.api import Cursor
from helpers.logging_config import get_logger
from time import sleep, time
import json


class FaceBookDataContainer:
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
        self.logger = get_logger(name=self.__class__.__name__)
        self.accounts = defaultdict(list)
        self.failed_jobs = []
        self.reporting_data = defaultdict(list)
        self.ads = []
        self.failed_accounts = defaultdict(list)
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

        # retreive insights, accounts are returned if ad report is needed
        results, failed_jobs = run_async_jobs(facebook_objects, self.retrieve_insights, fields, params)
        async_needed_accounts = [acct for acct in results if isinstance(acct, AdAccount)]

        # run async reports
        if async_needed_accounts:
            finished_reports_or_accounts, failed_async_jobs = run_async_jobs(
                async_needed_accounts, self.process_async_report, fields, params
            )
            # accounts that have failed after retrying multiple times
            failed_accounts = [acct for acct in finished_reports_or_accounts if isinstance(acct, AdAccount)]
            self.failed_jobs.extend([*failed_jobs, failed_async_jobs])
            results.extend(finished_reports_or_accounts)
            self.failed_accounts[date_preset].extend(failed_accounts)

        # process cursors (RESULTS)
        results = [result for result in results if isinstance(result, Cursor)]
        for result in results:
            self.extract_business_use_case_usage(result.headers())
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
            if error.get("code", None) in [1, 2, 100]:  # async needed
                return facebook_object
            self.logger.exception(e)
            return facebook_object

    def process_async_report(self, facebook_object, fields, params, max_attempts=5, initial_delay=60):
        # creates an async report, waits for it to finish, and returns the reports. Retries up to 5 times with an exponential backoff.
        attempts = 0
        success = False
        self.logger.info(f"Processing Async report for object id: {facebook_object['id']}.")
        while attempts <= max_attempts:
            attempts += 1
            report = self.call_method(facebook_object, "get_insights_async", fields, params)
            report = self.wait_for_job(report)
            if report[AdReportRun.Field.async_status] == "Job Completed":
                success = True
                break
            self.logger.info(
                f"Async report for facebook object id: {facebook_object['id']} unsuccessful.\nAsync_status of {report[AdReportRun.Field.async_status]}.\nRetrying in {initial_delay} seconds. Attempt {attempts} of {max_attempts}."
            )
            sleep(initial_delay)
            initial_delay *= 2

        if success:
            return self.unpack_and_return_report_results(facebook_object, report)

        self.logger.info(
            f"Async report for facebook object id: {facebook_object['id']} failed.\nAsync_status of {report[AdReportRun.Field.async_status]}.\{attempts} attempts"
        )
        return facebook_object

    def unpack_and_return_report_results(self, facebook_object, report, max_attempts=3, initial_delay=20):
        success = False
        attempts = 0
        while not success and attempts <= max_attempts:
            attempts += 1
            try:
                return report.get_result(params={"limit": 300})
            except FacebookRequestError as e:
                if e.body()["error"].get("code", None) in [1, 2, 4, 17, 341]:
                    self.logger.info(
                        f"Encountered error while unpacking results.\nFacebook object id: {facebook_object['id']}.\nError {e.body()['error']}.\nSleeping {initial_delay} seconds then retrying"
                    )
                    sleep(initial_delay)
                else:
                    raise (e)
            initial_delay *= 2
        self.logger.info(
            f"Failed to unpack results for facebook object id: {facebook_object['id']}.\n{attempts} attempts"
        )
        return facebook_object

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
        with open("data/ads_with_issues.json", "w") as f:
            json.dump(self.ads, f, default=lambda x: x.export_all_data(), indent=4)
        return self


class GHLDataContainer:
    def __init__(self, agency):
        self.logger = get_logger(self.__class__.__name__)
        self.agency = agency
        self.locations = []
        self.calendars = {}
        self.appointments = defaultdict(list)
        self.failed_futures = []
        super().__init__()

    def set_locations(self):
        locations = self.call_method(self.agency, "get_locations")
        self.locations = locations
        return self

    def set_location_calendars(self, locations):
        self.logger.info("Retreiving Calendars")
        success, failed = run_async_jobs(
            locations, lambda job: (job["id"], self.call_method(job, "get_calendar_services"))
        )
        for location_id, location_calendars in success:
            self.calendars[location_id] = location_calendars
        if failed:
            self.failed_futures.extend(failed)
        self.logger.info(f"Successfully retrieved {len(locations)-len(failed)} of {len(locations)}")
        return self

    def set_location_appointments(self, appointment_params):
        self.logger.info("Retreiving Appointments")
        for location_id, calendars in self.calendars.items():
            appts = []
            for calendar in calendars:
                query = self.call_method(calendar, "get_appointments", appointment_params)
                appts.extend(query)
                self.logger.debug(f"Retrieved {len(query)} appointments for calender id: {calendar['id']}")
            self.appointments[location_id] = appts
            self.logger.info(f"Retrieved {len(appts)} for location id: {location_id}")
        return self

    def call_method(self, ghl_object, method_name, *args, **kwargs):
        """
        GHL's database fails a ton. This retries every method_name three times
        """
        success = False
        attempts = 0
        initial_delay = 1
        method = getattr(ghl_object, method_name)
        while not success or attempts <= 3:
            attempts += 1
            try:
                result = method(*args, **kwargs)
                return result
            except GHLRequestError:
                self.logger.info(
                    f"GHL {ghl_object.__class__.__name__}, id: {ghl_object['id']}, method: {method.__name__}. Attempt {attempts} out of 3"
                )
                sleep(initial_delay)

        self.logger.info(
            f"Failed to call method {method.__name__} on {ghl_object.__class__.__name__} with id: {ghl_object['id']} after {attempts} attempts"
        )
        return None

    def export_all_data_csv(self):
        """
        Exports all data to a csv file.
        """
        self.logger.info("Writing all data to csv")
        self.export_locations_csv()
        self.export_calendars_csv()
        self.export_appointments_csv()
        return self

    def export_calendars_csv(self):
        fieldnames = {
            key for calendar_list in self.calendars.values() for calendar in calendar_list for key in calendar.keys()
        }
        write_dicts_to_csv(
            [cal for cal_list in self.calendars.values() for cal in cal_list],
            fieldnames,
            lambda cal: {key: cal[key] for key in cal},
            "data/calendars.csv",
        )
        return self

    def export_locations_csv(self):
        fieldnames = {key for location in self.locations for key in location.keys()}
        write_dicts_to_csv(
            self.locations, fieldnames, lambda loc: {key: loc[key] for key in loc}, "data/locations.csv"
        )
        return self

    def export_appointments_csv(self):
        fieldnames = {key for appt_list in self.appointments.values() for appt in appt_list for key in appt.keys()}
        write_dicts_to_csv(
            [appt for appt_list in self.appointments.values() for appt in appt_list],
            fieldnames,
            lambda appt: {key: appt[key] for key in appt},
            "data/appointments.csv",
        )
        return self

    def export_all_data_json(self):
        """
        Exports all data to a json file.
        """
        self.logger.info("Writing all data to json")
        self.export_locations_json()
        self.export_calendars_json()
        self.export_appointments_json()
        return self

    def export_calendars_json(self):
        with open("data/calendars.json", "w") as file:
            json.dump(self.calendars, file, indent=4, default=lambda x: x.export_all_data())
        return self

    def export_locations_json(self):
        with open("data/locations.json", "w") as file:
            json.dump(self.locations, file, indent=4, default=lambda x: x.export_all_data())
        return self

    def export_appointments_json(self):
        with open("data/appointments.json", "w") as file:
            json.dump(self.appointments, file, indent=4, default=lambda x: x.export_all_data())
        return self
