import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from transformation.data import process_appointments_data  # Import the function

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

date = st.date_input("Select a date", value=now.date())

df = process_appointments_data(date)

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
    }
    </style>
""", unsafe_allow_html=True)

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