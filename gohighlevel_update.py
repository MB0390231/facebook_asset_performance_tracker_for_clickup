from gohighlevel_python_sdk.ghlobjects.objects import Agency
from clickup_python_sdk.api import ClickupClient
from clickup_python_sdk.clickupobjects.folder import Folder
from helpers.datacontainers import GHLDataContainer
from helpers.reportbuilder import ReportBuilder, process_appointments
from helpers.clickupjobpool import ClickupJobPool
from utils import datetime_to_epoch, generate_datetime_string, current_date

import os

GHL_KEY = os.environ.get("GHL_KEY")
NORTHSTAR_FOLDER_ID = os.environ.get("NORTHSTAR_FOLDER_ID")
CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN")

CU = ClickupClient.init(user_token=CLICKUP_TOKEN)
NORTHSTAR_FOLDER = Folder(NORTHSTAR_FOLDER_ID).get()
NORTH_LISTS = NORTHSTAR_FOLDER.get_lists()
NORTHSTAR = [l for l in NORTH_LISTS if l["name"] == "Northstar"][0]
LOGS = [l for l in NORTH_LISTS if l["name"] == "Logs"][0].get_tasks()
TEMP = [t for t in LOGS if t["name"] == "temp"][0]
NORTHSTAR_TASKS = NORTHSTAR.get_tasks()
NORTHSTAR_FIELDS = {field["name"]: field["id"] for field in NORTHSTAR.get_custom_fields()}

LOCATION_IDS = [
    cf["value"]
    for tasks in NORTHSTAR_TASKS
    for cf in tasks["custom_fields"]
    if cf["name"].lower() == "location id" and cf.get("value", False)
]

appointment_params = {
    "startDate": datetime_to_epoch(generate_datetime_string(day_delta=-30)),
    "endDate": datetime_to_epoch(generate_datetime_string(day_delta=120)),
    "includeAll": True,
}

agency = Agency(agency_token=GHL_KEY)
GHL = GHLDataContainer(agency=agency)
GHL = GHLDataContainer(agency=agency).set_locations()
GHL.set_location_calendars(locations=[l for l in GHL.locations if l["id"] in LOCATION_IDS]).set_location_appointments(
    appointment_params
).export_all_data_json()

APPT_REPORT = ReportBuilder(default_report={"appt 7": 0, "appt fut": 0})
for locs, appts in GHL.appointments.items():
    APPT_REPORT.add_asset(locs, appts, process_appointments)
CUPOOL = ClickupJobPool(CU)
for task in NORTHSTAR_TASKS:
    CUPOOL.create_ghl_jobs(task, NORTHSTAR_FIELDS, APPT_REPORT)
LOGS = {"attachment": (f"{current_date()}.log", open(f"logs/{current_date()}.log", "rb"))}
TEMP.upload_file(LOGS)
