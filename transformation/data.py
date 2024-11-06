import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Function to create a Snowflake session
def create_snowflake_session():
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "role": st.secrets["snowflake"]["role"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"],
    }
    return Session.builder.configs(connection_parameters).create()

session = create_snowflake_session()


@st.cache_data(show_spinner=False)
def get_appointments():
    appointments_query = """
        SELECT created_at, id, area, NULL AS GOALS, NULL AS PROFILE_PICTURE FROM analytics.reporting.tbl_master_opportunities WHERE CREATED_AT >= DATEADD("day", -30, CURRENT_DATE) AND area IN ('Salem', 'Portland North', 'Des Moines', 'Minneapolis', 'Portland', 'Pasco', 'Medford', 'Bozeman', 'Cincinnati', 'Helena', 'Cedar Rapids', 'Missoula', 'Puget Sound', 'Spokane', 'Bend', 'Billings', 'Utah')
    """
    return session.sql(appointments_query).to_pandas()

def process_appointments_data(selected_date):
    # Fetch appointments data
    df = get_appointments()

    # Convert 'CREATED_AT' to date in df
    df['CREATED_AT'] = pd.to_datetime(df['CREATED_AT']).dt.date

    # Goals mapping
    goals = {
        'Salem': 25,
        'Portland North': 25,
        'Des Moines': 25,
        'Minneapolis': 10,
        'Portland': 15,
        'Pasco': 10,
        'Medford': 10,
        'Bozeman': 10,
        'Cincinnati': 15,
        'Helena': 5,
        'Cedar Rapids': 10,
        'Missoula': 10,
        'Puget Sound': 10,
        'Spokane': 10,
        'Bend': 5,
        'Billings': 5,
        'Utah': 5
    }

    # Create a date range for the past 30 days
    date_range = pd.date_range(end=datetime.today(), periods=30)

    # Create goals_df to include each date in the date_range
    goals_df = pd.DataFrame(
        [(area, goal, date.date(), 'https://i.ibb.co/5R9N3Bs/DAILY-SET-GOALS.png') 
         for area, goal in goals.items() for date in date_range],
        columns=['AREA', 'GOALS', 'CREATED_AT', 'PROFILE_PICTURE']
    )

    # Ensure 'CREATED_AT' is a date field in goals_df
    goals_df['CREATED_AT'] = pd.to_datetime(goals_df['CREATED_AT']).dt.date

    # Drop 'GOALS' and 'PROFILE_PICTURE' columns from df to avoid column conflicts
    df = df.drop(columns=['GOALS', 'PROFILE_PICTURE'])

    # Merge appointments data with goals data
    df = df.merge(goals_df, on=['AREA', 'CREATED_AT'], how='outer')

    # Filter for the selected date
    df = df[df['CREATED_AT'] == selected_date]

    # Group by AREA and CREATED_AT, counting ID and using max for GOALS
    df_groupby = df.groupby(['AREA', 'CREATED_AT']).agg({
        'ID': 'count',
        'GOALS': 'max',
        'PROFILE_PICTURE': 'first'
    }).reset_index()

    # Fill NaNs in 'GOALS' column with 0 before casting to int
    df_groupby['GOALS'] = df_groupby['GOALS'].fillna(0).astype(int)
    df_groupby['ID'] = pd.to_numeric(df_groupby['ID'], errors='coerce').fillna(0).astype(int)

    # Calculate Percent of Total
    df_groupby['Percent of Total'] = (df_groupby['ID'] / df_groupby['GOALS']).round(2)

    return df_groupby