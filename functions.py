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

def get_access_token(CLIENT_ID,CLIENT_SECRET,TOKEN_URL):
    
    """Fetch the access token from Veracross API."""
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "academics.enrollments:list academics.assignments.grades:list behavior:list staff_faculty:list students:list master_attendance:list academics.student_assignments:list academics.classes:list classes.attendance:list"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(TOKEN_URL, data=data, headers=headers)

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print("Error fetching access token:", response.text)
        return None


def get_students(STUDENTS_URL,access_token):
    """Fetch all student data using pagination via headers."""
    access_token = access_token
    if not access_token:
        print("No access token")
        return

    all_students = []
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

        response = requests.get(STUDENTS_URL, headers=headers)
        # print("got response for student list")

        if response.status_code == 200:
            students = response.json()
            if students["data"] == []:
                break


            grade_level = {item["id"] : item["description"] for item in students["value_lists"][1]["items"]}
            # print("grade_level",grade_level)

            homeroom = {item["id"] : item["description"] for item in students["value_lists"][3]["items"]}
            # print("homeroom",homeroom)

            gender = {item["id"] : item["description"] for item in students["value_lists"][0]["items"]}
            # print("gender",gender)

            enrollment_status = {int(item["id"]) : item["description"] for item in students["value_lists"][6]["items"]}




            for entry in students["data"]:
                # if "birthday" in entry:
                #     try:
                #         birthday = datetime.strptime(entry["birthday"], "%Y-%m-%d").date()
                #         entry["Age Today"] = calculate_detailed_age(birthday)
                #     except ValueError:
                #         entry["Age Today"] = "Invalid Date" 
                # if "roles" in entry:
                #     entry["Child of Staff/Faculty"] = check_child_of_faculty(entry["roles"])

                if entry["grade_level"] in grade_level:
                    entry["grade_level"] = grade_level[entry["grade_level"]]

                if entry["homeroom"] in homeroom:
                    entry["homeroom"] = homeroom[entry["homeroom"]]

                if entry["gender"] in gender:
                    entry["gender"] = gender[entry["gender"]]
                
                if entry["enrollment_status"] in enrollment_status:
                    entry["enrollment_status"] = enrollment_status[entry["enrollment_status"]]
                


            all_students.extend(students["data"])
            page += 1  # Go to the next page
        else:
            print("Error fetching students:", response.text)
            break

    print(f"Total students fetched: {len(all_students)}")

    df = pd.DataFrame(all_students)
    df["full_name"] = (
        df["last_name"] + ", " + df["first_name"]
    )

    
    return df

def get_all_teachers(TEACHERS_URL,access_token):
    """Fetch all teachers and store their emails in a dictionary."""
    access_token = access_token
    if not access_token:
        print("Failed to get access token")
        return {}

    teachers_dict = {}
    page_number = 1

    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page_number),
            "X-Page-Size": "1000"
        }

        response = requests.get(TEACHERS_URL, headers=headers)
        # if response.status_code != 200 or not response.json():
        teachers_data = response.json()
        if teachers_data["data"] == []:
            break  # No more data

        for teacher in teachers_data["data"]:
            # print(teacher)
            # print(type(teacher))
            # teachers_dict[teacher["id"]] = teacher["email_1"]  # Map teacher ID to email
            teachers_dict[teacher["id"]] = f'{teacher["last_name"]}, {teacher["first_name"]}'
        print("printed this")

        page_number += 1
    # print(teachers_dict)
    return teachers_dict



def get_hs_classes(CLASS_URL,access_token):
    """Fetch all teachers and store their emails in a dictionary."""
    access_token = access_token
    if not access_token:
        print("Failed to get access token")
        return {}

    all_class = []
    page_number = 1

    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page_number),
            "X-Page-Size": "1000"
        }
        params = {
            "school_level" : 4,
            "school_year" : 2025
        }

        response = requests.get(CLASS_URL, headers=headers, params=params)
        if response.status_code != 200 or not response.json():
            break

        # print(response.json())
        class_data = response.json()

        if class_data["data"] == []:
            break  # No more data

        # for cls in class_data["data"]:
        #     # print(teacher)
        #     # print(type(teacher))
        #     # teachers_dict[teacher["id"]] = teacher["email_1"]  # Map teacher ID to email
        #     all_class[cls["id"]] = f'{cls["class_id"]}, {cls["description"]}'

        print("printed this class")

        all_class.extend(class_data["data"])
        page_number += 1  # Go to the next page
    # print(teachers_dict)
    df = pd.DataFrame(all_class)
    df["class"] = (
    df["class_id"] + ": " + df["description"]
    )

    # df = df[df["id","class","primary_teacher_name"]]

    return df

