# transformation/sales_data.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Function to create a Snowflake session
@st.cache_resource
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

@st.cache_data(ttl=600)
def gs_query():

    session = create_snowflake_session()

    gs_query = """
        SELECT MAX(IFNULL("opportunities.current_month_sales_calc", 0) + IFNULL("opportunities.current_month_assists", 0)) current_month_sales_and_assists, MAX(IFNULL("opportunities.previous_month_sales_calc", 0) + IFNULL("opportunities.previous_month_assists", 0)) previous_month_sales_and_assists, "user.name" name, "team_members.effective_date" date, IFNULL("user.picture_link", 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730865202/Group_1127_zhbvez.png') picture_link, 8 GOAL
        FROM analytics.reporting.tbl_team_members
        WHERE "user.name" IN ('Orlando Sanchez', 'Devon Fish', 'Ryan Reeves', 'Jack McCoy', 'Mariana Flores', 'John Ray', 'Seth Groth', 'Jordon Price', 'Brady Reinwald', 'Steven Gutierrez', 'Kevin Judd', 'Bryan Bodnar', 'Tanner Gottschalk', 'Justin Clayton', 'Joseph Schuh', 'Carson Beus', 'Austin Lowe', 'Caleb Phillips', 'Enzo Couillens', 'Brooks Haas', 'Rian Wright', 'Brendan Lee', 'Nathan Hall', 'John Roach', 'Derek Jensen', 'Jackson Rodgers', 'Justic Brewton', 'Kyle Giffen', 'Skylar Adams', 'Michael Larrosa', 'Daniel Blankenship', 'Benjamin Toala', 'Houston Castillo', 'Mark Gransee', 'Austen Nine', 'Bret Nelson', 'Chris Bouchard', 'Michael Browne')
        GROUP BY ALL
        QUALIFY ROW_NUMBER() OVER(PARTITION BY "user.name" ORDER BY "team_members.effective_date" DESC) = 1
    """
    return session.sql(gs_query).to_pandas()