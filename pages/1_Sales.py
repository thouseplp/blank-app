import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from snowflake.snowpark.functions import col
from transformation.sales_data import process_sales_data  # Import the function
from features.progress_bar import sales_target  # Import sales_target function

# Set up Streamlit page configuration
st.set_page_config(
    page_title="Daily Set Goals",
    layout="wide"
)

st.logo("https://res.cloudinary.com/dwuzrptk6/image/upload/v1732139306/d97489eb-0834-40e3-a5b5-e93c2f0066b3_1-removebg-preview_1_z60jh6.png", size="large")

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

# Set the local timezone
local_tz = pytz.timezone('America/Los_Angeles')  # Replace with your time zone

# Get the current date and time in the local timezone
now = datetime.now(local_tz)

# Sidebar for selecting month and year
st.sidebar.header("Select Month and Year")

# Month selector
month = st.sidebar.selectbox(
    "Month",
    options=list(range(1, 13)),
    format_func=lambda x: datetime(1900, x, 1).strftime('%B'),
    index=now.month - 1  # Default to current month
)

# Year selector (adjust the range as needed)
current_year = now.year
year = st.sidebar.selectbox(
    "Year",
    options=list(range(current_year - 5, current_year + 1)),  # Past 5 years + current year
    index=5  # Default to current year (assuming the list starts 5 years ago)
)

def get_sales_data(month, year):
    return process_sales_data(month, year)

# Process sales data based on selected month and year
df = get_sales_data(month, year)

# Create columns to display sales targets
num_columns = 3  # Adjust as needed
columns = st.columns(num_columns)

# Iterate over each row in the dataframe and display sales targets
for idx, row in df.iterrows():
    with columns[idx % num_columns]:
        sales_target(
            area=row['AREA'],
            pace=row['PACE'],
            minimum_target=row['MIN_GOAL'],
            maximum_target=row['MAX_GOAL'],  # Adjust if necessary
            actual=row['ID'],
            image=row['PROFILE_PICTURE']
        )