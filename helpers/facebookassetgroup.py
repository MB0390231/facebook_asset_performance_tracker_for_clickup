from helpers.logging_config import BaseLogger
from helpers import helpers
from collections import defaultdict
from typing import List, Dict


class ReportBuilder:

    """
    This class is meant to build reports.
    """

    def __init__(self, default_report) -> None:
        self.default_report = default_report
        self.report = defaultdict(lambda: self.default_report.copy())
        return

    def add_asset(self, asset_name, asset, method):
        """
        consolidates the stats of an asset given with the stats already in the report for that asset_name

        asset_name: string used to target the report for an asset
        asset: asset to add to the targeted report
        method: method used to consolidate the asset with the target
        """
        target = self.report[asset_name]
        asset = method(target, asset)
        return

    def get_asset(self, asset_name):
        if asset_name not in self.report:
            return self.default_report
        return self.report[asset_name]


def consolidate_facebook_reporting_data(target_stat, obj):
    """
    Takes in a report for facebook insights and adsinsights objects and combines the statistics of both.
    """
    strategy_mapping = {
        "spend": (lambda x, y: float(x) + float(y), False),
        "actions": (combine_actions, False),
        "inline_link_click_ctr": (weighted_average, True),
        "cpc": (weighted_average, True),
        "cpm": (weighted_average, True),
        "cost_per_inline_link_click": (weighted_average, True),
    }

    original_spend = float(target_stat["spend"])
    for metric, (method, requires_weighted_average) in strategy_mapping.items():
        if metric not in obj:
            continue

        if requires_weighted_average:
            values = [target_stat[metric], float(obj[metric])]
            weights = [original_spend, float(obj["spend"])]
            target_stat[metric] = method(values=values, weights=weights)
        else:
            target_stat[metric] = method(target_stat[metric], obj[metric])

    target_stat["count"] += 1
    return target_stat


def weighted_average(values, weights):
    "order matters regarding values and weights"
    if len(values) != len(weights):
        raise ValueError("The number of values must be equal to the number of weights")
    if sum(weights) == 0:
        raise ValueError("The sum of weights cannot be zero")
    return sum([values[i] * weights[i] for i in range(len(values))]) / sum(weights)


def combine_actions(report: Dict[str, int], list_of_actions_objects: List[Dict[str, str]]) -> Dict[str, int]:
    """
    Combines the values of actions in a list of action objects into a report.

    :param report: The initial report to which values will be added.
    :type report: Dict[str, int]
    :param list_of_actions_objects: A list of action objects, each containing an "action_type" and a "value".
    :type list_of_actions_objects: List[Dict[str, str]]
    :return: The combined report.
    :rtype: Dict[str, int]

    :Example:
    >>> combine_actions({"like": 1}, [{"action_type": "like", "value": "2"}, {"action_type": "share", "value": "3"}])
    {"like": 3, "share": 3}
    """
    report = report.copy()
    for actions in list_of_actions_objects:
        action = actions["action_type"]
        value = int(actions["value"])
        report[action] = report.get(action, 0) + value
    return report
