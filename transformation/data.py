import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import plotly.graph_objects as go

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


@st.cache_data(show_spinner=False, ttl=600)
def get_appointments():
    appointments_query = """
        SELECT created_at, id, area, NULL AS GOALS, NULL AS PROFILE_PICTURE FROM analytics.reporting.tbl_master_opportunities WHERE CREATED_AT >= DATEADD("day", -30, CURRENT_DATE) AND area IN ('Salem', 'Portland North', 'Des Moines', 'Minneapolis', 'Portland', 'Pasco', 'Medford', 'Bozeman', 'Cincinnati', 'Helena', 'Cedar Rapids', 'Missoula', 'Puget Sound', 'Spokane', 'Bend', 'Billings', 'Utah') AND lead_generator IS NOT NULL AND lead_generator != closer
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
    # Profile picture URLs mapping
    profile_pictures = {
        'Salem': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863720/salem_eckoe1.png',
        'Portland North': None,  # Using Portland image
        'Des Moines': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863717/des_moines_mcwvbz.png',
        'Minneapolis': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863713/minneapolis_jlnpqw.png',
        'Portland': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/portland_iwid9m.png',
        'Pasco': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863718/pasco_fxdzsg.png',
        'Medford': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863720/medford_ks5ol1.png',
        'Bozeman': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/bozeman_z1dcyw.png',
        'Cincinnati': None,  # No URL provided
        'Helena': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863713/helena_b0lpfy.png',
        'Cedar Rapids': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730865480/Group_1128_bckfag.png',
        'Missoula': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863715/missoulda_lmfros.png',  # Assuming misspelling
        'Puget Sound': None,  # No specific URL provided
        'Spokane': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863715/spokane_i8tixp.png',
        'Bend': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/bend_dvre85.png',
        'Billings': 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863716/billings_hezzk6.png',
        'Utah': None  # No URL provided
    }

    # Default profile picture URL for areas without a specific image
    default_profile_picture = 'https://res.cloudinary.com/dwuzrptk6/image/upload/v1730865202/Group_1127_zhbvez.png'
    
    # Create a date range for the past 30 days
    date_range = pd.date_range(end=datetime.today(), periods=30)

    # Create goals_df to include each date in the date_range
    goals_df = pd.DataFrame(
        [(area, goal, date.date(), profile_pictures.get(area) if profile_pictures.get(area) else default_profile_picture) 
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

    df_groupby = df_groupby.sort_values(by='AREA')

    return df_groupby

st.components.v1.html(f"""
<div style="background-color: #393B41; padding: 20px; border-radius: 20px; width: 100%; box-sizing: border-box; color: white; font-family: Arial, Helvetica, sans-serif;">
    <!-- Expander starts here -->
    <details style="width: 95%; margin-bottom: 10px;">
        <summary style="display: flex; justify-content: space-between; align-items: center; font-weight: bold; font-size: 18px; cursor: pointer; list-style: none; outline: none;">
            <div style="display: flex; align-items: center;">
                <img src="https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/bend_dvre85.png" style="width: 40px; height: 40px; margin-right: 10px;">
                Bend
                <span style="font-size: 14px; color: #6E7076; margin-left: 5px;">&#9660;</span> <!-- Downward arrow caret -->
            </div>
            <div style="text-align: right; color: #2C966C;">50% 📈</div>
        </summary>
        <!-- Additional content inside the expander -->
        <div style="margin-top: 10px; color: white; font-size: 14px;">
            <!-- Add your additional information here -->
            <p>This is additional information inside the expander. You can include any content here, such as more details, explanations, or data.</p>
        </div>
    </details>
    <!-- Expander ends here -->
    <!-- "6 on pace to 14" line -->
    <div style="width: 95%; text-align: left; margin-bottom: 8px; font-size: 18px; font-weight: bold;">
        6<span style="font-size: 14px; font-weight: normal; color: #6E7076;"> on pace to 14</span>
    </div>
    <!-- Progress bar container -->
    <div style="width: 95%; position: relative; display: flex; background: #333; border-radius: 20px; height: 20px; overflow: hidden;">
        <!-- Green section -->
        <div style="background: #2C966C; width: 40%;"></div>
        <!-- Grey sections -->
        <div style="background: #303135; width: 32%;"></div>
        <div style="background: #303135; width: 28%;"></div>
        <!-- Vertical line at the 12 mark -->
        <div style="position: absolute; left: 72%; top: 0; height: 100%; width: 2px; background-color: #FFFFFF;"></div>
    </div>
    <!-- Numbers below the chart -->
    <div style="width: 95%; display: flex; justify-content: space-between; margin-top: 8px; font-weight: bold; font-size: 14px;">
        <div style="width: 40%; text-align: left;"></div> <!-- Placeholder for spacing -->
        <div style="width: 32%; text-align: right;">12</div>
        <div style="width: 28%; text-align: right;">15</div>
    </div>
</div>
""", height=250)