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
from datetime import date
today = date.today().isoformat()
print(today,"date")


def get_daily_attendance(DAILY_ATTENDANCE_URL,access_token,student_df):
    """Fetch all student data using pagination via headers."""
    access_token = access_token
    if not access_token:
        print("No access token")
        return

    attendance = []
    page = 1
    page_size = 1000  # Max allowed is 1000, but we start with 100
    print("here")
    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page),
            "X-Page-Size": str(page_size),
            "X-API-Value-Lists" : "include"

            # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
        }

        params = {
            "attendance_date": date.today().isoformat()
        }

        response = requests.get(DAILY_ATTENDANCE_URL, headers=headers, params=params)
        print("got response for attendance")

        if response.status_code == 200:
            attendance_data = response.json()
            if attendance_data["data"] == []:
                break
            

            print(len(attendance_data["data"]))
            attendance_data["data"] = [
                attendance
                for attendance in attendance_data["data"]
                if attendance.get("attendance_category") != 0
            ]
            print(len(attendance_data["data"]))
            print("inside")
            attendance_category = {int(item["id"]) : item["description"] for item in attendance_data["value_lists"][0]["items"]}
            student_attendance_status = {item["id"] : item["description"] for item in attendance_data["value_lists"][1]["items"]}
            print("got it")
            # print(type(entry["attendance_category"]))
            print(type(attendance_category),attendance_category)
            # print("Yes") if attendance_data["data"]["attendance_category"] == 0 else "No"
            for entry in attendance_data["data"]:
                if entry["attendance_category"] in attendance_category:
                    entry["attendance_category"] = attendance_category[entry["attendance_category"]]

                if entry["student_attendance_status"] in student_attendance_status:
                    entry["student_attendance_status"] = student_attendance_status[entry["student_attendance_status"]]
                entry["excused"] = "Yes" if entry["excused"] is True else "No"


            print("after for")
            attendance.extend(attendance_data["data"])
            page += 1  
            print("page")
        else:
            print("Error fetching students:", response.text)
            break

    df = pd.DataFrame(attendance)
    # df = map_teacher(df,teachers_dict=teachers_dict)
    df = map_student_grade(df,student_df=student_df)
    df = df[df["grade_level"].isin(["Grade 9", "Grade 10","Grade 11", "Grade 12"])]
    print(df,"fdf")
    
    return df



def map_student_grade(attendance,student_df):
    student_df = student_df.rename(columns={"id": "student_id_for_merge"})

    attendance = attendance.merge(
        student_df[["student_id_for_merge", "grade_level"]],
        left_on="person_id",
        right_on="student_id_for_merge",
        how="left"
    )
    attendance = attendance.drop(columns=["student_id_for_merge"])


    return attendance