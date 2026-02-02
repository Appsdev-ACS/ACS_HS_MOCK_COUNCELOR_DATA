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


# Get current date
today = datetime.today()
current_year = today.year
current_month = today.month

# Calculate school year
school_year = current_year - 1 if current_month <= 7 else current_year


    



# def get_assignments(ASSIGNMENT_URL,access_token,student_df,class_df):
#     """Fetch all student data using pagination via headers."""
#     access_token = access_token
#     if not access_token:
#         print("No access token")
#         return

#     assignments = []
#     page = 1
#     page_size = 1000  # Max allowed is 1000, but we start with 100
#     while True:
#     # while page == 1:


#         headers = {
#             "Authorization": f"Bearer {access_token}",
#             "X-Page-Number": str(page),
#             "X-Page-Size": str(page_size),
#             "X-API-Value-Lists" : "include"

#             # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
#         }
#         params = {
#             "school_year" : school_year
#         }

#         response = requests.get(ASSIGNMENT_URL, headers=headers, params=params)
#         print("got response for assignment list")

#         if response.status_code == 200:
#             assignment_data = response.json()
#             if assignment_data["data"] == []:
#                 break

#             # assignment_data["internal_class_id"] = assignment_data["data"]["assignment"].get("internal_class_id")
#             # assignment_data["grading_period_id"] = assignment_data["data"]["assignment"].get("grading_period_id")
#             # assignment_data["assignment_type"] = assignment_data["data"]["assignment"].get("assignment_type")
#             # assignment_data["due_date"] = assignment_data["data"]["assignment"].get("due_date")

#             if page == 1:
#                 grading_periods = {item["id"] : item["description"] for item in assignment_data["value_lists"][0]["items"]}
#                 assignment_type = {item["id"] : item["description"] for item in assignment_data["value_lists"][1]["items"]}
#                 completion_status = {item["id"] : item["description"] for item in assignment_data["value_lists"][3]["items"]}
#             for entry in assignment_data["data"]:
#                 assignment = entry.get("assignment", {})
#                 # print(assignment)
#                 # print(assignment.keys())

#                 entry["internal_class_id"] = assignment.get("internal_class_id")
#                 entry["grading_period_id"] = assignment.get("grading_period_id")
#                 entry["assignment_type"] = assignment.get("assignment_type")
#                 entry["due_date"] = assignment.get("due_date")
#                 entry["description"] = assignment.get("description")

#                 if entry["grading_period_id"] in grading_periods:
#                     entry["grading_period_id"] = grading_periods[entry["grading_period_id"]]
#                 if entry["assignment_type"] in assignment_type:
#                     entry["assignment_type"] = assignment_type[entry["assignment_type"]]
#                 if entry["completion_status"] in completion_status:
#                     if entry["completion_status"] not in (4, 6):
#                         entry["completion_status"] = completion_status[entry["completion_status"]]


#             assignments.extend(assignment_data["data"])
#             page += 1  
#         else:
#             print("Error fetching students:", response.text)
#             break

#     # print(f"Total students fetched: {len(assignments)}")
#     # print(behavior)
#     df = pd.DataFrame(assignments)
#     df = df[~df['completion_status'].isin([4, 6])]
#     df = df[df["grading_period_id"].str.startswith("HS", na=False)]
#     df = map_class(assignments=df,class_df=class_df)
#     df = map_students(assignments=df,student_df=student_df)
#     df = extract_test(df)
#     df = extract_proficiency(df)
#     df = extract_status(df)

#     print(df,"fdf")
    
#     return df



def get_assignments(ASSIGNMENT_URL,access_token,student_df,class_df):
# def fetch_assignments(access_token, school_year, ASSIGNMENT_URL):
    if not access_token:
        print("No access token")
        return []

    assignments = []
    page = 1
    page_size = 1000

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {access_token}",
        "X-Page-Size": str(page_size),
    })

    params = {"school_year": school_year}

    grading_periods = assignment_type = completion_status = None

    while True:
        session.headers["X-Page-Number"] = str(page)

        # include value lists ONLY on first page
        if page == 1:
            session.headers["X-API-Value-Lists"] = "include"
        else:
            session.headers.pop("X-API-Value-Lists", None)

        resp = session.get(ASSIGNMENT_URL, params=params, timeout=60)
        if resp.status_code != 200:
            print("Error fetching assignments:", resp.text)
            break

        data = resp.json()
        rows = data.get("data", [])
        if not rows:
            break

        # Build maps once from page 1
        if page == 1 and "value_lists" in data:
            vl = data["value_lists"]
            # NOTE: keep your indexes if you're sure they're stable; safer is to locate by name/type if available
            grading_periods = {i["id"]: i["description"] for i in vl[0]["items"]}
            assignment_type  = {i["id"]: i["description"] for i in vl[1]["items"]}
            completion_status = {i["id"]: i["description"] for i in vl[3]["items"]}

        gp = grading_periods or {}
        at = assignment_type or {}
        cs = completion_status or {}

        for entry in rows:
            a = entry.get("assignment") or {}
            entry["internal_class_id"] = a.get("internal_class_id")
            entry["grading_period_id"] = gp.get(a.get("grading_period_id"), a.get("grading_period_id"))
            entry["assignment_type"]   = at.get(a.get("assignment_type"), a.get("assignment_type"))
            entry["due_date"]          = a.get("due_date")
            entry["description"]       = a.get("description")

            # completion_status is on entry (not inside assignment)
            status_id = entry.get("completion_status")
            if status_id in cs and status_id not in (4, 6):
                entry["completion_status"] = cs[status_id]

        assignments.extend(rows)
        page += 1

    # return assignments


    # print(f"Total students fetched: {len(assignments)}")
    # print(behavior)
    df = pd.DataFrame(assignments)
    df = df[~df['completion_status'].isin([4, 6])]
    df = df[df["grading_period_id"].str.startswith("HS", na=False)]
    df = map_class(assignments=df,class_df=class_df)
    df = map_students(assignments=df,student_df=student_df)
    df = extract_test(df)
    df = extract_proficiency(df)
    df = extract_status(df)

    print(df,"fdf")
    
    return df


def map_class(assignments, class_df):
    class_df = class_df.rename(columns={"id": "class_id_to_merge"})
    # print(class_df)

    assignments = assignments.merge(
        class_df[["class_id_to_merge","class", "primary_teacher_name"]],
        left_on="internal_class_id",
        right_on="class_id_to_merge",
        how="left"
    )
    assignments = assignments.drop(columns=["class_id_to_merge"])


    return assignments

def map_students(assignments,student_df):
    student_df = student_df.rename(columns={"id": "student_id_for_merge"})

    assignments = assignments.merge(
        student_df[["student_id_for_merge", "full_name","grade_level"]],
        left_on="student_id",
        right_on="student_id_for_merge",
        how="left"
    )
    assignments = assignments.drop(columns=["student_id_for_merge"])


    return assignments


def extract_test(df):
    def map_task(value):
        if pd.isna(value) or value == "":
            return ""
        if str(value).startswith(("L T", "LT")):
            return "Learning Task"
        elif str(value).startswith(("S", "S-", "Su")):
            return "Summative"
        elif str(value).startswith(("F", "F-")):
            return "Formative"
        else:
            return "Other"
    
    df['Test'] = df['assignment_type'].apply(map_task)
    return df


def extract_proficiency(df):
    df['Proficiency'] = df['completion_status'].apply(lambda x: x if pd.notna(x) and "Proficiency" in str(x) else "")
    return df

def extract_status(df):
    df['Status'] = df['completion_status'].apply(lambda x: "" if pd.notna(x) and "Proficiency" in str(x) else x)
    return df
