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
from behavior import get_behavior
from assignments import get_assignments
from daily_attendance import get_daily_attendance
from functions import get_access_token,get_all_teachers,get_students,get_hs_classes
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor


import os
import asyncio
from flask import Flask, jsonify
from class_attendance import fetch_class_attendance


app = Flask(__name__)
load_dotenv()

# Configuration
TOKEN_URL = "https://accounts.veracross.com/acsad/oauth/token"
BEHAVIOR_URL = "https://api.veracross.com/ACSAD/v3/behavior"
TEACHERS_URL = "https://api.veracross.com/ACSAD/v3/staff_faculty"
STUDENTS_URL = "https://api.veracross.com/ACSAD/v3/students"
DAILY_ATTENDANCE_URL = "https://api.veracross.com/ACSAD/v3/master_attendance"
ASSIGNMENT_URL = "https://api.veracross.com/ACSAD/v3/academics/student_assignments"
CLASS_URL = "https://api.veracross.com/ACSAD/v3/academics/classes"


CLIENT_ID = os.getenv("CLIENT_ID")

CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# SERVICE_ACCOUNT_FILE = "student-data-latest key(service account).json"  # Update this path
SPREADSHEET_NAME = "HS Mock Counselor Data"

def upload_to_google_sheets(df,sheet_name,clear_sheet):
    """Uploads the DataFrame to Google Sheets."""
    # creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    creds, _ = default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)

    # Open or create the Google Sheet
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SPREADSHEET_NAME).worksheet(sheet_name)  # Create a new sheet

    # Clear existing data
    if(clear_sheet) == True:
        sheet.clear()

        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        sheet.update("A1", values)   # ✅ much faster

    else:
        values = df.astype(str).values.tolist()  # no header, just data
        sheet.append_rows(values, value_input_option="USER_ENTERED")

    print("✅ Data uploaded in one batch!")

def behavior(behavior_df):
    behavior_df.rename(columns={"student_id": "STUDENT: Person ID","id":"Behavior ID","incident_date" :"Incident Date","incident_type": "Incident Type","reporting_person":"Reporting Person","incident_notes":"Incident Notes","status":"Status","status_date":"Status Date","full_name":"Student","grade_level":"Current Grade" }, inplace=True)
														
    column_order = ["STUDENT: Person ID","Behavior ID","Student","Current Grade","Incident Date","Incident Type","Reporting Person","Incident Notes","Status","Status Date"]
    behavior_df = behavior_df.reindex(columns=column_order)
    behavior_df = behavior_df.fillna("")
    upload_to_google_sheets(behavior_df,sheet_name = "BehaviorComments",clear_sheet=True)
    # behavior_df.to_csv('test.csv')

def daily_attendance(daily_attendance_df):
    # PERSON: Person ID	Attendance Date	Grade	Person	Attendance Category	Status (Students)	Excused	Notes	SLR Details	Student				
    # id	attendance_date	person_id	person	attendance_category	student_attendance_status	faculty_attendance_status	excused	late_arrival_time	early_dismissal_time	return_time	school_year	grading_period	day_of_week	notes	last_modified_date	grade_level																					
    daily_attendance_df.rename(columns={"person_id": "PERSON: Person ID","attendance_date":"Attendance Date","grade_level" :"Grade","person": "Person","attendance_category":"Attendance Category","student_attendance_status":"Status (Students)","excused":"Excused","notes":"Notes"}, inplace=True)
														
    column_order = ["PERSON: Person ID","Attendance Date","Grade","Person","Attendance Category","Status (Students)","Excused","Notes","SLR Details"]
    daily_attendance_df = daily_attendance_df.reindex(columns=column_order)
    daily_attendance_df = daily_attendance_df.fillna("")
    upload_to_google_sheets(daily_attendance_df,sheet_name = "DailyAttendance",clear_sheet=False)


def assignment(assignment_df):
    assignment_df.rename(columns={"student_id": "STUDENT: Person ID","grading_period_id":"Grading Period","full_name" :"Student","grade_level": "STUDENT: Current Grade","class":"Class","primary_teacher_name":"Teacher","assignment_type":"Assignment Type","due_date":"Due Date","completion_status":"Completion Status","raw_score":"Raw Score","description":"Description" }, inplace=True)
														
    column_order = ["STUDENT: Person ID","Grading Period","Student","STUDENT: Current Grade","Class","Teacher","Assignment Type","Due Date","Description","Completion Status","Numeric Grade","Raw Score","Test","Proficiency","Status"]
    assignment_df = assignment_df.reindex(columns=column_order)
    assignment_df = assignment_df.fillna("")
    upload_to_google_sheets(assignment_df,sheet_name="StudentAssignmentGrades",clear_sheet=True)



def class_attendace(access_token,class_df):


    # Example: internal class IDs (replace with real list)
    # class_ids = [15717,16124,15718]
    class_ids = class_df["id"].tolist()


    attendance_data = asyncio.run(
        fetch_class_attendance(
            class_ids=class_ids,
            access_token=access_token
        )
    )
    if not attendance_data:
        return pd.DataFrame()

    # flatten nested fields
    df = pd.json_normalize(attendance_data)

    # optional: flatten block dict into separate columns
    if "block" in df.columns:
        block_df = pd.json_normalize(df["block"])
        block_df = block_df.add_prefix("block_")
        df = df.drop(columns=["block"]).join(block_df)

    upload_to_google_sheets(df,sheet_name="ClassAttendance",clear_sheet=False)


    # success = sum(1 for r in attendance_data if "attendance" in r)
    # failed = len(attendance_data) - success

    return jsonify({
        "status": "completed",
        "total_classes": len(class_ids)
        # "success": success,
        # "failed": failed
    })




def fetch_behavior(BEHAVIOR_URL,access_token,student_df,teachers_dict):
    return get_behavior(BEHAVIOR_URL, access_token, student_df, teachers_dict)

def fetch_daily_attendance(access_token,student_df):
    return get_daily_attendance(DAILY_ATTENDANCE_URL, access_token, student_df)

def fetch_assignments(access_token, student_df, class_df):
    return get_assignments(ASSIGNMENT_URL, access_token, student_df, class_df)

def fetch_students(access_token):
    return get_students(STUDENTS_URL,access_token)

def fetch_teachers(access_token):    
    return get_all_teachers(TEACHERS_URL,access_token)

def fetch_class(access_token):
    return get_hs_classes(CLASS_URL,access_token)



def main():
    access_token = get_access_token(CLIENT_ID, CLIENT_SECRET, TOKEN_URL)

    # Step 1: fetch students, teachers, classes in parallel
    with ThreadPoolExecutor() as executor:
        students_future = executor.submit(fetch_students, access_token)
        teachers_future = executor.submit(fetch_teachers, access_token)
        classes_future = executor.submit(fetch_class, access_token)

        try:
            student_df = students_future.result()
        except Exception as e:
            print(f"❌ Error fetching students: {e}")
            student_df = pd.DataFrame()  # fallback empty DF

        try:
            teachers_dict = teachers_future.result()
        except Exception as e:
            print(f"❌ Error fetching teachers: {e}")
            teachers_dict = {}  # fallback empty dict

        try:
            class_df = classes_future.result()
        except Exception as e:
            print(f"❌ Error fetching classes: {e}")
            class_df = pd.DataFrame()  # fallback empty DF

    # Step 2: fetch main data in parallel with error handling
    with ThreadPoolExecutor() as executor:
        futures = []

        # Behavior
        def safe_behavior():
            try:
                df = fetch_behavior(BEHAVIOR_URL, access_token, student_df, teachers_dict)
                behavior(df)
            except Exception as e:
                print(f"❌ Error fetching or processing behavior: {e}")

        futures.append(executor.submit(safe_behavior))

        # Daily Attendance
        def safe_attendance():
            try:
                df = fetch_daily_attendance(access_token, student_df)
                daily_attendance(df)
            except Exception as e:
                print(f"❌ Error fetching or processing daily attendance: {e}")

        futures.append(executor.submit(safe_attendance))

        # Assignments
        def safe_assignment():
            try:
                df = fetch_assignments(access_token, student_df, class_df)
                assignment(df)
            except Exception as e:
                print(f"❌ Error fetching or processing assignments: {e}")

        futures.append(executor.submit(safe_assignment))

        # Wait for all to complete (optional)
        for f in futures:
            f.result()


@app.route("/run")
def run_job():
    try:
        main()  # call your existing main() function
        return "✅ Student data uploaded successfully!", 200
    except Exception as e:
        return f"❌ Error: {e}", 500

if __name__ == "__main__":
    # app.run() #staging
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
