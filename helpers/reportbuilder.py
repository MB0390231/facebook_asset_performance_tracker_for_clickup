from datetime import datetime, timedelta
from collections import defaultdict, abc
from typing import List, Dict, Callable, Any, Optional, Union
import pytz
from datetime import datetime, timedelta
from dateutil.parser import parse


class ReportBuilder(abc.MutableMapping):

    """
    This class is used to build reports by consolidating statistics of assets with existing reports.

    Attributes:
        default_report (Any): The default report to be used when building a new report for an asset.
        report (defaultdict): The dictionary that contains all the reports, with asset names as keys and reports as values.
    """

    def __init__(self, default_report) -> None:
        self.default_report = default_report
        self.report = defaultdict(lambda: self.default_report.copy())

    def __getitem__(self, asset_name):
        return self.report[asset_name]

    def __setitem__(self, asset_name, value):
        self.report[asset_name] = value

    def __delitem__(self, asset_name):
        del self.report[asset_name]

    def __iter__(self):
        return iter(self.report)

    def __len__(self):
        return len(self.report)

    def add_asset(self, asset_name: str, asset: Any, method: Callable) -> None:
        """
        Consolidates the statistics of an asset with the statistics already in the report for the given asset name.

        Args:
            asset_name (str): The name of the asset used to target the report.
            asset (Any): The asset to add to the targeted report.
            method (Callable): The method used to consolidate the asset with the target. The method should take in two arguments: the report dictionary and the asset to be consolidated.

        Example:
            report = Report()
            asset_name = "Asset1"
            asset = {
                "spend": 20.0,
                "actions": {
                    "action1": 1,
                    "action2": 2,
                },
            }
            method = consolidate_asset
            report.add_asset(asset_name, asset, method)
            print(report)
            # Output:
            # {
            #     "Asset1": {
            #         "spend": 20.0,
            #         "actions": {
            #             "action1": 1,
            #             "action2": 2,
            #         },
            #     },
            # }
        """
        report_dict = self[asset_name]
        asset = method(report_dict, asset)


def add_ads_with_issues(report_dict, asset):
    if asset["configured_status"] != "ACTIVE":
        return report_dict
    report_dict["issues"] += 1
    return report_dict


def consolidate_facebook_reporting_data(report_dict: Dict, obj: Dict) -> Dict:
    """
    Consolidates Facebook reporting data from two sources and combines the statistics.

    Args:
        report_dict (Dict): A dictionary representing the original report data, containing the following keys:
            "spend" (float): The total spend.
            "actions" (Dict[str, int]): A dictionary of actions.
            "unique_actions" (Dict[str, int]): A dictionary of unique actions.
            "inline_link_click_ctr" (float): The inline link click CTR.
            "cpc" (float): The cost per click.
            "cpm" (float): The cost per thousand impressions.
            "cost_per_inline_link_click" (float): The cost per inline link click.
            "count" (int): The count of the data.
        obj (Dict): A dictionary representing the additional report data to be consolidated, containing the following keys:
            "spend" (float): The total spend.
            "actions" (Dict[str, int], optional): A dictionary of actions.
            "unique_actions" (Dict[str, int], optional): A dictionary of unique actions.
            "inline_link_click_ctr" (float, optional): The inline link click CTR.
            "cpc" (float, optional): The cost per click.
            "cpm" (float, optional): The cost per thousand impressions.
            "cost_per_inline_link_click" (float, optional): The cost per inline link click.

    Returns:
        Dict: The updated report dictionary, containing the consolidated data.

    """
    strategy_mapping = {
        "spend": (lambda x, y: float(x) + float(y), False),
        "actions": (combine_actions, False),
        "unique_actions": (combine_actions, False),
        "inline_link_click_ctr": (weighted_average, True),
        "cpm": (weighted_average, True),
        "cost_per_inline_link_click": (weighted_average, True),
    }

    original_spend = float(report_dict["spend"])
    for metric, (method, requires_weighted_average) in strategy_mapping.items():
        if metric not in obj:
            continue

        if requires_weighted_average:
            values = [report_dict[metric], float(obj[metric])]
            weights = [original_spend, float(obj["spend"])]
            report_dict[metric] = method(values=values, weights=weights)
        else:
            report_dict[metric] = method(report_dict[metric], obj[metric])
    # TODO: change this quick fix hacky solution
    report_dict["cost_per_lead"] = (
        report_dict["spend"] / report_dict["actions"]["lead"] if "lead" in report_dict["actions"].keys() else None
    )
    report_dict["cost_per_purchase"] = (
        report_dict["spend"] / report_dict["actions"]["purchase"]
        if "purchase" in report_dict["actions"].keys()
        else None
    )
    report_dict["count"] += 1
    return report_dict


def weighted_average(values, weights):
    "order matters regarding values and weights"
    if len(values) != len(weights):
        raise ValueError("The number of values must be equal to the number of weights")
    if sum(weights) == 0:
        raise ValueError("The sum of weights cannot be zero")
    return sum([values[i] * weights[i] for i in range(len(values))]) / sum(weights)


def combine_actions(report: Dict[str, int], list_of_actions_objects: List[Dict[str, str]]) -> Dict[str, int]:
    """
    Combines a list of actions objects into a report.

    Args:
        report (Dict[str, int]): The report to be updated, represented as a dictionary.
        list_of_actions_objects (List[Dict[str, str]]): A list of dictionaries, where each dictionary represents a single action and contains the following keys:
            "action_type" (str): The type of the action.
            "value" (str): The value of the action, represented as a string.

    Returns:
        Dict[str, int]: The updated report dictionary, containing the combined actions.

    Example:
        report = {
            "action1": 0,
            "action2": 0,
        }
        list_of_actions_objects = [
            {"action_type": "action1", "value": "1"},
            {"action_type": "action2", "value": "2"},
            {"action_type": "action1", "value": "3"},
        ]
        result = combine_actions(report, list_of_actions_objects)
        print(result)
        # Output: {'action1': 4, 'action2': 2}
    """
    report = report.copy()
    for actions in list_of_actions_objects:
        action = actions["action_type"]
        value = int(actions["value"])
        report[action] = report.get(action, 0) + value
    return report


def process_appointments(
    report_dict: Dict[str, int], appointments: List[Dict[str, Union[str, Optional[str]]]]
) -> Dict[str, int]:
    """
    Processes appointments and updates the report dictionary with the number of appointments in the future and the number of appointments created in the past 7 days.

    Args:
        report_dict (Dict[str, int]): A dictionary that will be updated with the number of appointments in the future and the number of appointments created in the past 7 days. The dictionary should have the following keys:
            "appt fut" (int): The number of appointments in the future.
            "appt 7" (int): The number of appointments created in the past 7 days.
        appointments (List[Dict[str, Union[str, Optional[str]]]]): A list of dictionaries, where each dictionary represents a single appointment and contains the following keys:
            "appointmentStatus" (str): The status of the appointment.
            "startTime" (str): The start time of the appointment.
            "createdAt" (str): The creation time of the appointment.
            "selectedTimezone" (str, optional): The timezone of the appointment, specified as a string in the format "Area/Location".

    Returns:
        Dict[str, int]: The updated report dictionary, containing the number of appointments in the future and the number of appointments created in the past 7 days.

    Example:
        report_dict = {
            "appt fut": 0,
            "appt 7": 0,
        }
        appointments = [
            {"appointmentStatus": "confirmed", "startTime": "2023-03-30T10:00:00Z", "createdAt": "2023-03-23T10:00:00Z"},
            {"appointmentStatus": "confirmed", "startTime": "2023-04-01T10:00:00Z", "createdAt": "2023-03-20T10:00:00Z"},
            {"appointmentStatus": "cancelled", "startTime": "2023-03-29T10:00:00Z", "createdAt": "2023-03-20T10:00:00Z"},
        ]
        result = process_appointments(report_dict, appointments)
        print(result)
        # Output: {'appt fut': 1, 'appt 7': 1}
    """
    local_tz = datetime.now().astimezone().tzinfo

    today = datetime.now().replace(tzinfo=local_tz)
    start_of_today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    one_week_ago = start_of_today - timedelta(days=7)

    for appointment in appointments:
        if appointment["appointmentStatus"] != "confirmed":
            continue
        created_at = parse(appointment["createdAt"])
        start_time = parse(appointment["startTime"])

        if "selectedTimezone" in appointment:
            timezone = pytz.timezone(appointment["selectedTimezone"])
            created_at = created_at.astimezone(timezone)
            start_time = start_time.astimezone(timezone)
            start_of_today = start_of_today.astimezone(timezone)
            one_week_ago = one_week_ago.astimezone(timezone)

        if start_time > start_of_today:
            report_dict["appt fut"] += 1

        if one_week_ago <= created_at < start_of_today:
            report_dict["appt 7"] += 1

    return report_dict
