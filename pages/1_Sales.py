import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from snowflake.snowpark import Session
from transformation.sales_data import process_sales_data  # Update process_sales_data to accept a session
from features.progress_bar import sales_target  # Import sales_target function

# ----------------------------
# Page configuration and styling
# ----------------------------
st.set_page_config(
    page_title="Daily Set Goals",
    layout="wide"
)

# Replace st.logo (which isn’t officially supported) with st.image
st.image("https://res.cloudinary.com/dwuzrptk6/image/upload/v1732139306/d97489eb-0834-40e3-a5b5-e93c2f0066b3_1-removebg-preview_1_z60jh6.png", width=200)

# Hide default Streamlit elements for a cleaner look
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

# ----------------------------
# Time zone setup and sidebar controls
# ----------------------------
local_tz = pytz.timezone('America/Los_Angeles')
now = datetime.now(local_tz)

st.sidebar.header("Select Month and Year")
month = st.sidebar.selectbox(
    "Month",
    options=list(range(1, 13)),
    format_func=lambda x: datetime(1900, x, 1).strftime('%B'),
    index=now.month - 1  # Default to current month
)

current_year = now.year
year = st.sidebar.selectbox(
    "Year",
    options=list(range(current_year - 5, current_year + 1)),
    index=5  # Default to current year (assuming the list starts 5 years ago)
)

# ----------------------------
# Step 1: Create a Cached Snowflake Session
# ----------------------------
@st.cache_resource
def get_snowflake_session():
    # Get your connection parameters securely from st.secrets
    connection_parameters = st.secrets["snowflake"]
    session = Session.builder.configs(connection_parameters).create()
    return session

# ----------------------------
# Step 2: Update Data Retrieval to Use the Cached Session
# ----------------------------
def get_sales_data(month, year):
    session = get_snowflake_session()  # Get the cached Snowflake session
    # Call process_sales_data with the session; ensure your function is updated accordingly
    return process_sales_data(session, month, year)

# Retrieve sales data based on selected month and year
df = get_sales_data(month, year)

# ----------------------------
# Step 3: Display Sales Targets in Columns
# ----------------------------
num_columns = 3  # Adjust the number of columns as needed
columns = st.columns(num_columns)

# Iterate over each row in the dataframe and display a sales target card
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
