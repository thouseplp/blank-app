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
        SELECT created_at, id, area FROM analytics.reporting.tbl_master_opportunities WHERE CREATED_AT >= DATEADD("day", -30, CURRENT_DATE)
    """
    return session.sql(appointments_query).to_pandas()

def process_appointments_data(selected_date):
    # Fetch appointments data
    df = get_appointments()

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

    # Create goals DataFrame and add today's date
    goals_df = pd.DataFrame(list(goals.items()), columns=['AREA', 'GOALS'])
    goals_df['CREATED_AT'] = datetime.today().date()
    goals_df['PROFILE_PICTURE'] = 'https://i.ibb.co/5R9N3Bs/DAILY-SET-GOALS.png'  # Add profile image link


    # Concatenate appointments and goals data
    df = pd.concat([df, goals_df], ignore_index=True)

    # Fill missing GOALS values with 0 if needed
    df['GOALS'] = df['GOALS'].fillna(0).round(0)

    df = df[df['CREATED_AT'] == selected_date]

    # Group by AREA and CREATED_AT, counting ID and using max for GOALS
    df_groupby = df.groupby(['AREA', 'CREATED_AT']).agg({
        'ID': 'count',
        'GOALS': 'max',
        'PROFILE_PICTURE': 'first'
    }).reset_index()

    # Ensure columns are numeric
    df_groupby['ID'] = pd.to_numeric(df_groupby['ID'], errors='coerce')
    df_groupby['GOALS'] = pd.to_numeric(df_groupby['GOALS'], errors='coerce')

    # Replace 0s in GOALS with NaN to avoid division by zero
    df_groupby['GOALS'] = df_groupby['GOALS'].replace(0, float('nan'))

    # Calculate Percent of Total
    df_groupby['Percent of Total'] = (df_groupby['ID'] / df_groupby['GOALS']).round(2)

    return df_groupby