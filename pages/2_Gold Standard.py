import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from transformation.gold_standard import gs_query
from features.progress_bar import gold_standard

# Set up Streamlit page configuration
st.set_page_config(
    page_title="Gold Standard",
    layout="wide"
)

st.logo("https://res.cloudinary.com/dwuzrptk6/image/upload/v1733871285/Asset_1_300x_1_ste6ag.png", size="large")

# Hide Streamlit's default menu, footer, and header
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

gs_df = gs_query()

month_options = ['This Month', 'Last Month']

with st.sidebar:
    month_selection = st.radio('Choose Month', month_options, horizontal=True, label_visibility="hidden")

# Sort the DataFrame by the selected month's data
if month_selection == 'This Month':
    gs_df = gs_df.sort_values(by='CURRENT_MONTH_SALES_AND_ASSISTS', ascending=False)
else:
    gs_df = gs_df.sort_values(by='PREVIOUS_MONTH_SALES_AND_ASSISTS', ascending=False)

# Create the layout for the cards
num_columns = 3  # Number of columns per row
columns = st.columns(num_columns)

# Iterate through the sorted DataFrame and place cards in the appropriate column
for idx, row in gs_df.reset_index(drop=True).iterrows():
    col_idx = idx % num_columns  # Determine which column to place the card
    with columns[col_idx]:
        # Determine the actual value based on the selected month
        if month_selection == 'This Month':
            actual_value = row['CURRENT_MONTH_SALES_AND_ASSISTS']
        else:
            actual_value = row['PREVIOUS_MONTH_SALES_AND_ASSISTS']

        gold_standard(
            name=row['NAME'],
            goal=row['GOAL'],
            actual=actual_value,
            profile_image=row['PICTURE_LINK']
        )