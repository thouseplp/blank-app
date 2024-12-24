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


def cv_query():
    cv_query = """
        SELECT
            id,
            sale_date "Sale Date",
            closer "Closer",
            closer_picture_link "Closer Picture Link",
            fm_picture_link "FM Picture Link",
            lead_generator "Field Marketer",
            area "Area",
            COUNT(DISTINCT CASE WHEN project_sub_category = 'Solar' THEN id END) "Solar",
            COUNT(DISTINCT CASE WHEN project_sub_category = 'Battery' THEN id END) "Batteries",
            COUNT(DISTINCT CASE WHEN project_sub_category = 'Roof' OR project_sub_category LIKE 'Reroof%' THEN id END) "Roofs",
            COUNT(DISTINCT CASE WHEN project_sub_category LIKE 'Solar +%' THEN id END) "Bundled",
            grand_total - lender_fee_total "CV"
        FROM analytics.reporting.tbl_master_opportunities
        WHERE sale_date >= '2024-12-01'
        GROUP BY ALL """
    return session.sql(cv_query).to_pandas()