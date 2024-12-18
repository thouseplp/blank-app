# transformation/sales_data.py
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

def leaderboard_query():
    leaderboard_query = """
        SELECT date "Date", closer "Closer", area "Area", COUNT(DISTINCT CASE WHEN metric = 'Sales' THEN id END) "Sales", COUNT(DISTINCT CASE WHEN metric = 'Sits' THEN id END) "Sits", COUNT(DISTINCT CASE WHEN metric = 'Opportunities' THEN id END) "Opps"
FROM analytics.team_reporting.dtbl_sales_leaderboard
GROUP BY ALL
    """
    return session.sql(leaderboard_query).to_pandas()

def fm_leaderboard_query():
    fm_leaderboard_query = """
        SELECT date "Date", lead_generator "FM", area "Area", COUNT(DISTINCT CASE WHEN metric = 'Sales' THEN id END) "Assists", COUNT(DISTINCT CASE WHEN metric = 'Sits' THEN id END) "Sits", COUNT(DISTINCT CASE WHEN metric = 'Sets' THEN id END) "Sets"
FROM analytics.team_reporting.dtbl_sales_leaderboard
GROUP BY ALL
ORDER BY "Assists"
    """
    return session.sql(fm_leaderboard_query).to_pandas()