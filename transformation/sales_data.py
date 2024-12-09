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

profile_pictures = {
    'Salem': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863720/salem_eckoe1.png',
    'Portland North': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730865202/Group_1127_zhbvez.png',
    'Des Moines': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863717/des_moines_mcwvbz.png',
    'Minneapolis': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863713/minneapolis_jlnpqw.png',
    'Portland': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/portland_iwid9m.png',
    'Pasco': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863718/pasco_fxdzsg.png',
    'Medford': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863720/medford_ks5ol1.png',
    'Bozeman': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/bozeman_z1dcyw.png',
    'Cincinnati': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1733779348/Cincinnati_2-Photoroom_rxg5dz.png',  # Updated to single entry
    'Helena': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863713/helena_b0lpfy.png',
    'Cedar Rapids': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730865480/Group_1128_bckfag.png',
    'Missoula': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863715/missoulda_lmfros.png',
    'Puget Sound': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1733779404/PugetSound-Photoroom_wmj5k7.png',  # Updated to single entry
    'Spokane': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863715/spokane_i8tixp.png',
    'Bend': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/bend_dvre85.png',
    'Billings': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863716/billings_hezzk6.png',
    'Utah': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1733780794/Group_1168_htnlaz.png'  # Updated to single entry
}

def get_sales(month, year):
    sales_query = """
        SELECT 
            sale_date,
            id,
            area,
            DAYOFMONTH(CURRENT_DATE) as day_of_month,
            RIGHT(LAST_DAY(CURRENT_DATE()),2) as days_in_month,
            1 - ROUND(DAYOFMONTH(CURRENT_DATE()) / RIGHT(LAST_DAY(CURRENT_DATE()),2),2) as percent_of_month_left,
            SUM(CASE WHEN sale_date >= DATEADD("day", -30, CURRENT_DATE()) THEN 1 ELSE 0 END) OVER(PARTITION BY area) as last_30_day_sales,
            SUM(CASE WHEN YEAR(sale_date) = YEAR(CURRENT_DATE()) AND MONTH(sale_date) = MONTH(CURRENT_DATE()) THEN 1 ELSE 0 END) OVER(PARTITION BY area) as current_month_sales,
            FLOOR((last_30_day_sales * percent_of_month_left) + current_month_sales) as pace
        FROM 
            analytics.reporting.tbl_master_opportunities
        WHERE sale_date >= DATEADD("month", -6, CURRENT_DATE())
            AND grand_total > 0
            AND area IN (
                'Salem', 
                'Portland North', 
                'Des Moines', 
                'Minneapolis', 
                'Portland', 
                'Pasco', 
                'Medford', 
                'Bozeman', 
                'Cincinnati', 
                'Helena', 
                'Cedar Rapids', 
                'Missoula', 
                'Puget Sound', 
                'Spokane', 
                'Bend', 
                'Billings', 
                'Utah'
            )
    """
    return session.sql(sales_query).to_pandas()

def process_sales_data(month, year):
    # Fetch sales data for the selected month and year
    df = get_sales(month, year)

    if df.empty:
        st.warning("No sales data available for the selected month and year.")
        return pd.DataFrame()

    # Convert 'SALE_DATE' to date in df
    df['SALE_DATE'] = pd.to_datetime(df['SALE_DATE']).dt.date

    pace_df = df.groupby('AREA').agg({'PACE': 'first'}).reset_index()

    # Goals mapping
    goals = {
        'Salem': {'min_goal': 80, 'max_goal': 96},
        'Portland North': {'min_goal': 65, 'max_goal': 78},
        'Des Moines': {'min_goal': 55, 'max_goal': 66},
        'Minneapolis': {'min_goal': 0, 'max_goal': 0},
        'Portland': {'min_goal': 40, 'max_goal': 48},
        'Pasco': {'min_goal': 35, 'max_goal': 42},
        'Medford': {'min_goal': 40, 'max_goal': 48},
        'Bozeman': {'min_goal': 20, 'max_goal': 24},
        'Cincinnati': {'min_goal': 24, 'max_goal': 29},
        'Helena': {'min_goal': 25, 'max_goal': 30},
        'Cedar Rapids': {'min_goal': 20, 'max_goal': 24},
        'Missoula': {'min_goal': 30, 'max_goal': 36},
        'Puget Sound': {'min_goal': 20, 'max_goal': 24},
        'Spokane': {'min_goal': 15, 'max_goal': 18},
        'Bend': {'min_goal': 15, 'max_goal': 18},
        'Billings': {'min_goal': 10, 'max_goal': 12},
        'Utah': {'min_goal': 10, 'max_goal': 12}
    }

    # Default profile picture URL
    default_profile_picture = 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730865202/Group_1127_zhbvez.png'
    
    # Define the start and end dates for the selected month and year
    start_date = datetime(year, month, 1).date()
    if month == 12:
        end_date = datetime(year + 1, 1, 1).date() - pd.Timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1).date() - pd.Timedelta(days=1)
    
    # Create a date range for the selected month
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Create goals_df without weekend/weekday logic
    goals_df_list = []
    for area, goals_values in goals.items():
        for date in date_range:
            min_goal = goals_values['min_goal']
            max_goal = goals_values['max_goal']
            goals_df_list.append({
                'AREA': area,
                'MIN_GOALS': min_goal,
                'MAX_GOALS': max_goal,
                'SALE_DATE': date.date(),
                'PROFILE_PICTURE': profile_pictures.get(area, default_profile_picture)
            })

    goals_df = pd.DataFrame(goals_df_list)

    # Merge sales data with goals data
    df = df.merge(goals_df, on=['AREA', 'SALE_DATE'], how='outer')

    # Filter for the selected date range
    df = df[(df['SALE_DATE'] >= start_date) & (df['SALE_DATE'] <= end_date)]

    daily_df = df.groupby(['AREA', 'SALE_DATE']).agg({
        'ID': 'count',
        'MIN_GOALS': 'first',  # Minimum goal is the same per area per day
        'MAX_GOALS': 'first',  # Maximum goal is the same per area per day
        'PROFILE_PICTURE': 'first'
    }).reset_index()

    # Sum over the date range per AREA
    df_groupby = daily_df.groupby(['AREA']).agg({
        'ID': 'sum',
        'MIN_GOALS': 'first',
        'MAX_GOALS': 'first',
        'PROFILE_PICTURE': 'first'
    }).reset_index()

    df_groupby = df_groupby.merge(pace_df, on='AREA', how='left')

    df_groupby['PACE'] = df_groupby['PACE'].fillna(0).astype(int)

    # Fill NaNs in 'GOALS' column with 0 before casting to int
    df_groupby['MIN_GOALS'] = df_groupby['MIN_GOALS'].fillna(0).astype(int)
    df_groupby['MAX_GOALS'] = df_groupby['MAX_GOALS'].fillna(0).astype(int)
    df_groupby['PROFILE_PICTURE'].replace('', np.nan, inplace=True)
    df_groupby['PROFILE_PICTURE'] = df_groupby['PROFILE_PICTURE'].fillna(default_profile_picture)
    df_groupby['ID'] = pd.to_numeric(df_groupby['ID'], errors='coerce').fillna(0).astype(int)

    # Calculate Percent of Total, handling division by zero
    df_groupby['PERCENT_OF_MIN_TOTAL'] = np.where(
        df_groupby['MIN_GOALS'] != 0,
        (df_groupby['ID'] / df_groupby['MIN_GOALS']).round(2),
        0
    )
    df_groupby['PERCENT_OF_MAX_TOTAL'] = np.where(
        df_groupby['MAX_GOALS'] != 0,
        (df_groupby['ID'] / df_groupby['MAX_GOALS']).round(2),
        0
    )

    df_groupby = df_groupby.sort_values(by='AREA')

    return df_groupby