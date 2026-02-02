import requests
import pandas as pd
import csv
from datetime import datetime,date
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
import os
from google.auth import default
import gspread
from flask import Flask
from datetime import datetime



    



def get_behavior(BEHAVIOR_URL,access_token,student_df,teachers_dict):
    """Fetch all student data using pagination via headers."""
    access_token = access_token
    if not access_token:
        print("No access token")
        return

    behavior = []
    page = 1
    page_size = 1000  # Max allowed is 1000, but we start with 100

    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page),
            "X-Page-Size": str(page_size),
            "X-API-Value-Lists" : "include"

            # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
        }

        response = requests.get(BEHAVIOR_URL, headers=headers)
        # print("got response for student list")

        if response.status_code == 200:
            behavior_data = response.json()
            if behavior_data["data"] == []:
                break

            incident_type = {item["id"] : item["description"] for item in behavior_data["value_lists"][0]["items"]}
            status = {item["id"] : item["description"] for item in behavior_data["value_lists"][1]["items"]}
            for entry in behavior_data["data"]:
                if entry["incident_type"] in incident_type:
                    entry["incident_type"] = incident_type[entry["incident_type"]]

                if entry["status"] in status:
                    entry["status"] = status[entry["status"]]


            behavior.extend(behavior_data["data"])
            page += 1  
        else:
            print("Error fetching students:", response.text)
            break

    print(f"Total students fetched: {len(behavior)}")
    # print(behavior)
    df = pd.DataFrame(behavior)
    df = map_teacher(df,teachers_dict=teachers_dict)
    df = map_students(df,student_df=student_df)
    print(df,"fdf")
    
    return df



def map_teacher(behavior, teachers_dict):
    """Map homeroom teacher emails to students."""
    # students_df["homeroom_teacher_email"] = students_df["homeroom_teacher_id"].apply(
    #     # lambda teacher: teachers_dict.get(teacher["id"], "N/A") if isinstance(teacher, dict) else "N/A"
    #     lambda teacher: teachers_dict[teacher["id"]] if isinstance(teacher, dict) else "N/A"

    # )
    print("here")
    print(type(behavior["reporting_person_id"]),"person id")

    print(behavior["reporting_person_id"],"person id")
    behavior["reporting_person"] = behavior["reporting_person_id"].map(teachers_dict)
    print("no problem")

    return behavior

def map_students(behavior,student_df):
    student_df = student_df.rename(columns={"id": "student_id_for_merge"})

    behavior = behavior.merge(
        student_df[["student_id_for_merge", "full_name","grade_level"]],
        left_on="student_id",
        right_on="student_id_for_merge",
        how="left"
    )
    behavior = behavior.drop(columns=["student_id_for_merge"])


    return behavior