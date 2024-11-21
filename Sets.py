import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import pytz
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from transformation.appointments_data import process_appointments_data  # Import the function

st.set_page_config(
    page_title="Daily Set Goals",
    layout="wide"
)

st.logo("https://i.ibb.co/5R9N3Bs/DAILY-SET-GOALS.png", size="large")

hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    .css-10trblm {padding-top: 0px; padding-bottom: 0px;}
    .css-1d391kg {padding-top: 0px !important;}
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

local_tz = pytz.timezone('America/Los_Angeles')  # Replace with your time zone

# Get the current date and time in your local time zone
now = datetime.now(local_tz)

st.sidebar.header("Select Date")

# Allow selecting a date range in the sidebar
date_range = st.sidebar.date_input(
    "",
    value=(now.date(), now.date())
)

# Debugging output
# st.write(f"date_range: {date_range}, type: {type(date_range)}")

# Function to flatten the date_range
def flatten_date_range(date_range_input):
    flat_dates = []
    if isinstance(date_range_input, (list, tuple)):
        for item in date_range_input:
            if isinstance(item, (datetime, date)):
                flat_dates.append(item)
            elif isinstance(item, (list, tuple)):
                flat_dates.extend(flatten_date_range(item))
            else:
                st.error("Invalid date selection.")
                st.stop()
    elif isinstance(date_range_input, (datetime, date)):
        flat_dates.append(date_range_input)
    else:
        st.error("Invalid date selection.")
        st.stop()
    return flat_dates

# Flatten the date_range
flat_date_list = flatten_date_range(date_range)

if len(flat_date_list) == 2:
    start_date, end_date = flat_date_list
elif len(flat_date_list) == 1:
    start_date = end_date = flat_date_list[0]
else:
    st.error("Please select a valid date or date range.")
    st.stop()

df = process_appointments_data((start_date, end_date))

from features.progress_bar import create_card  # Import create_card from progress_bar

st.markdown("""
    <style>
    .card {
        background-color: #41434A;
        padding: 10px;
        border-radius: 10px;
        margin-bottom: 5px;
        color: white;
        position: relative;
    }
    .profile-section {
        display: flex;
        align-items: center;
        margin-bottom: 8px;
    }
    .profile-pic {
        border-radius: 50%;
        width: 45px;
        height: 45px;
        margin-right: 15px;
    }
    .name {
        font-size: 16px;
        font-weight: bold;
    }
    .appointments {
        font-size: 16px;
        margin-bottom: 10px;
        color: white;
    }
    .progress-bar {
        background-color: #37383C;
        border-radius: 25px;
        width: 100%;
        height: 20px;
        position: relative;
        margin-bottom: 10px;
    }
    .progress-bar-fill {
        background-color: #C34547;
        height: 100%;
        border-radius: 25px;
    }
    .goal {
        position: absolute;
        right: 5px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 16px;
        color: white;
        font-weight: bold;
    ._container_51w34_1 {
        display: none !important;
    }
    }
    </style>
""", unsafe_allow_html=True)

# Calculate total goals and actuals
total_goals = df['GOALS'].sum()
total_actual = df['ID'].sum()

with st.container():
    create_card(
        area='All Areas',
        goal=total_goals,
        actual=total_actual,
        profile_image='https://res.cloudinary.com/dwuzrptk6/image/upload/v1732061718/Group_1147_ad1zmf.png',
        name='Purelight'
    )

columns = st.columns(3)
for idx, row in df.iterrows():
    with columns[idx % 3]:
        create_card(
            area=row['AREA'],
            goal=row['GOALS'],
            actual=row['ID'],
            profile_image=row['PROFILE_PICTURE'],
            name=row['AREA']
        )