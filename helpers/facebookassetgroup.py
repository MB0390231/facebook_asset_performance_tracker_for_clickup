from helpers.logging_config import BaseLogger
from helpers import helpers


class FacebookAssetGroups(BaseLogger):
    """
    An AssetGroup is a collection of assets
    An asset is a regex pattern meant to be found in an objs target_key
    """

    def __init__(self, data_container) -> None:
        self.data_container = data_container
        self.reports = {}
        return super().__init__()

    # TODO: instead of using asset group attributes, use a dictionary to store the asset groups with the report and data as values
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
        group = self.__getattribute__(asset_group_name)
        for preset in self.data_container.date_presets:
            for obj in self.data_container.insights_data[preset]:
                extracted = helpers.extract_regex_expression(obj[target_key], regex_pattern)
                if extracted:
                    asset_id = extracted.lower()
                    if asset_id not in group[preset].keys():
                        group[preset][asset_id] = [obj]
                    else:
                        group[preset][asset_id].append(obj)
            for keys in group[preset].keys():
                self.logger.debug(f"Found {len(group[preset][keys])} assets for {keys}")
        self.logger.info(f"Processed asset group {asset_group_name}")
        self.generate_asset_report(asset_group_name)
        return group

    def create_asset_group(self, asset_group):
        """
        Sets or resets the asset group to be used for the rest of the class
        """
        # set attr if not already set
        if not hasattr(self, f"{asset_group}"):
            self.__setattr__(f"{asset_group}", {})
        else:
            self.__setattr__(f"{asset_group}", {})
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
            self.logger.debug(f"Generating report asset group {asset_group} for date preset: {dates}")
            for keys, value in group[dates].items():
                report[dates][keys] = self.consolidate_objects_stats(value)
                self.logger.debug(f"{keys}: {report[dates][keys]}")
        self.reports[asset_group] = report
        self.logger.info(f"Generated report for asset group {asset_group}")
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
        cpc_values = []
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
            cpc_values.append(float(obj.get("cpc", 0)))
            ctr_values.append(float(obj.get("inline_link_click_ctr", 0)))
            cpm_values.append(float(obj.get("cpm", 0)))

        # calculate cost per actions
        for actions in consolidation.get("actions", []):
            consolidation[f"cost_per_{actions}"] = consolidation["spend"] / consolidation["actions"][actions]

        # calculate averages for CPC, CPM, and CTR
        consolidation["cpc"] = helpers.weighted_average(cpc_values, weights)
        consolidation["cpm"] = helpers.weighted_average(cpm_values, weights)
        consolidation["ctr"] = helpers.weighted_average(ctr_values, weights)
        return consolidation
