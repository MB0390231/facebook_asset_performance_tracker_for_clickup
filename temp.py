from keys import clickup_token
from clickup_python_sdk.api import ClickupClient
import helpers
import csv
import time
import math


def csv_to_json(csv_file_path):
    # Open the CSV file and read the header
    with open(csv_file_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        header = csv_reader.fieldnames

        # Create a list to hold the JSON objects
        json_list = []

        # Iterate over each row in the CSV file
        for row in csv_reader:
            # Create a dictionary to hold the values for this row
            row_dict = {}

            # Add each value to the dictionary
            for key in header:
                row_dict[key] = row[key]

            # Convert the dictionary to a JSON object and add it to the list
            json_list.append(row_dict)

        # Return the list of JSON objects
        return json_list


def dollars_to_number(dollar_string):
    # Remove the dollar sign and commas
    stripped_string = dollar_string.replace("$", "").replace(",", "")

    # Convert the string to a float
    number = float(stripped_string)

    # Return the number
    return number


north = csv_to_json("json/Copy of North Star - All Data.csv")


RGM_CLICKUP = ClickupClient.init(user_token=clickup_token)
NORTHSTAR = helpers.get_clickup_list(
    client=RGM_CLICKUP,
    team_name="ReDefined Growth Marketing",
    space_name="North Star",
    folder_name="rgm-northstar-clickup",
    list_name="Northstar",
)
NS_TASKS = NORTHSTAR.get_tasks()
NS_FIELDS = helpers.custom_field_dict(NORTHSTAR.get_custom_fields())

status = {
    "Live": "650f33d5-1377-42a3-a69e-e6a6cc6856ec",
    "Pre-Launch": "3182f3e3-bd27-42a4-9aa9-c77ea9ca5df5",
    "Churn": "d799a384-69f3-47e0-89b1-68ba2e1f40ec",
    "Paused": "16d09e83-7b8b-462e-87e3-afae2ee0d721",
}

offline = {
    "Offline": "3b537912-38c6-45f1-868f-cf19b483613e",
    "Online": "0aebb388-d7b2-4b7f-8d50-5b50bda74d8b",
}

scope = {
    "Ads & Lead Nurturing": "41f3d70f-3207-4155-91a2-f97d656b2071",
    "Ads Only": "f354ae42-459c-4d3b-87a5-af0388ddaf5b",
}

source = {
    "Lead Form": "bb7ebff8-9464-4db5-b425-b286b59db3af",
    "$0 Deposit Funnel": "cb081ce5-204c-4f65-a83d-32ce244c6ebb",
    "Payment Funnel": "ac01198a-0409-4b99-81a6-dde0da098699",
}

strategy = {
    "Split by Campaign": "1697d110-f2e7-42bf-812f-c736e6e54450",
    "Split by Ad Set": "1e7e5e79-7aab-4b3e-bfe2-ed99c0671e31",
    "Split by Ad Set - DA": "5dd4ca2d-ce1b-4d7e-ac1f-e978687f2e7e",
    "Combined": "3e3c7282-653d-44c1-b26a-c227ef560d27",
    "N/A": "8616600b-d1a9-4bfb-8386-220a10e2afc2",
    "Slimming only": "c32fb13b-079c-4dbd-a928-b3443e6a00f6",
    "Facial Only": "67f2e929-8dd2-4026-838a-734feec51984",
}

people = {
    "kyle": "57055330",
    "callie": "26197352",
    "spencer": "4458587",
    "eric": "60645027",
    "beka": "14920578",
    "ethan": "38225579",
}

complete = False
while not complete:
    RGM_CLICKUP.refresh_rate_limit()

    for retailers in north[15:18]:
        rate = int(RGM_CLICKUP.RATE_LIMIT_REMAINING)
        reset = int(RGM_CLICKUP.RATE_RESET)
        if rate < 9:
            sleep = reset - time.time()
            if sleep > 0:
                # loading_bar(jobs_ran, len(jobs), time_to_sleep=math.ceil(sleep))
                print(f"Sleeping for {math.ceil(sleep)} seconds...")
                helpers.sleep_and_print(math.ceil(sleep))
        for task in NS_TASKS:
            retailer_id = helpers.get_custom_field_value(input_list=task["custom_fields"], name="ID")
            if retailer_id == retailers["ID"]:
                print(f"Attempting retailer {retailer_id}")
                # # update notes
                # task.update_custom_field(custom_field_id=NS_FIELDS["Notes"], value=retailers["Media Buyer Notes:"])
                # task.update_custom_field(custom_field_id=NS_FIELDS["RGM Notes"], value=retailers["Other Notes:"])
                # update mb
                if retailers["Senior MB"].lower() in people.keys():
                    task.update_custom_field(
                        custom_field_id=NS_FIELDS["SMB"], value={"add": [people[retailers["Senior MB"].lower()]]}
                    )
                    print(f"Senior MB found for {retailers['ID']}")
                # Update Revenue
                if retailers["Rev 14"] != "":
                    task.update_custom_field(
                        custom_field_id=NS_FIELDS["Revenue 14"], value=dollars_to_number(retailers["Rev 14"])
                    )
                else:
                    print(f"No revenue found for {retailer_id}")
                # update Status
                task.update_custom_field(
                    custom_field_id=NS_FIELDS["Status"], value=status[retailers["Hand & Stone (Cranberry Township)"]]
                )
                print(f"Status found for {retailers['ID']}")
                # update Budget

                if retailers["Budget"] != "":
                    task.update_custom_field(
                        custom_field_id=NS_FIELDS["Budget"], value=dollars_to_number(retailers["Budget"])
                    )
                    print(f"Budget found for {retailers['ID']}")
                # update online/offline

                if retailers["Offline?"] == "FALSE":
                    task.update_custom_field(custom_field_id=NS_FIELDS["Offline?"], value=offline["Offline"])
                elif retailers["Offline?"] == "TRUE":
                    task.update_custom_field(custom_field_id=NS_FIELDS["Offline?"], value=offline["Online"])

                print(f"Offline found for {retailers['ID']}")

                # update Scope
                if retailers["Scope of Work"] != "":
                    task.update_custom_field(
                        custom_field_id=NS_FIELDS["Scope"], value=scope[retailers["Scope of Work"]]
                    )
                    print(f"Scope found for {retailers['ID']}")
                # update Source
                if retailers["Source"] != "":
                    task.update_custom_field(
                        custom_field_id=NS_FIELDS["Source"], value=source[retailers["Source"].rstrip()]
                    )
                print(f"Source found for {retailers['ID']}")
                # update Strategy
                if retailers["Strategy"] != "":
                    task.update_custom_field(
                        custom_field_id=NS_FIELDS["Strategy"], value=strategy[retailers["Strategy"].rstrip()]
                    )
                    print(f"Strategy found for {retailers['ID']}")
                # offer
                if retailers["Offer"] != "":
                    task.update_custom_field(custom_field_id=NS_FIELDS["Offer"], value=retailers["Offer"])
                    print(f"Offer found for {retailers['ID']}")
                # Restore Agency Account
                if retailers["Restore Agency Account"] == "TRUE":
                    task.update_custom_field(custom_field_id=NS_FIELDS["Restore Agency Account"], value=True)

                print(f"Restore Agency Account found for {retailers['ID']}")

    complete = True
