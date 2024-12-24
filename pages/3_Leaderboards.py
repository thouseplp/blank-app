import streamlit as st
import datetime
from transformation.leaderboard import leaderboard_query, fm_leaderboard_query
from transformation.contract_value_leaderboard import cv_query

# Set up Streamlit page configuration
st.set_page_config(
    page_title="Leaderboards",
    layout="wide"
)

st.logo("https://res.cloudinary.com/dwuzrptk6/image/upload/v1735062262/Mask_group_84_mw9j9s.png", size="large")

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

# Pull data from queries
df = leaderboard_query()         # Energy Consultant data
fm_df = fm_leaderboard_query()   # Field Marketer data
df_cv = cv_query()               # Contract Value data

# Pre-aggregate the Contract Value dataframe
df_cv = (
    df_cv.groupby(by="Closer")
         .agg({
             'CV': 'sum',
             'Solar': 'sum',
             'Batteries': 'sum',
             'Roofs': 'sum',
             'Bundled': 'sum'
          })
         .sort_values(by='CV', ascending=False)
         .reset_index()
)
df_cv['CV'] = df_cv['CV'].round(0)

# Get unique areas
unique_areas = sorted(df["Area"].unique())

# Calculate default date range (first of current month - last day of current month)
today = datetime.date.today()
if today.month == 12:
    next_month = datetime.date(today.year + 1, 1, 1)
else:
    next_month = datetime.date(today.year, today.month + 1, 1)
end_of_month = next_month - datetime.timedelta(days=1)
end_of_month_day = end_of_month.day

# Sidebar-like controls
cols1, cols2, cols3, cols4 = st.columns([1,1,1,1])

with cols1:
    date_range = st.date_input(
        "Date", 
        (datetime.date(today.year, today.month, 1), 
         datetime.date(today.year, today.month, end_of_month_day))
    )
    start_date, end_date = date_range

with cols2:
    area = st.multiselect("Area", unique_areas)

with cols3:
    dimension = st.selectbox("Dimension", ('Rep', 'Area'))

with cols4:
    role = st.selectbox("Role", ('Energy Consultant', 'Field Marketer'))   

# Filter Energy Consultant data based on date and area
df_filtered = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]
if area:
    df_filtered = df_filtered[df_filtered["Area"].isin(area)]

# Group Energy Consultant data
if dimension == 'Rep':
    ec_grouped_df = (
        df_filtered
        .groupby(["Closer","Closer Picture Link"], as_index=False)
        [['Sales', 'Sits', 'Opps']].sum()
        .sort_values('Sales', ascending=False)
    )
else:  # dimension == 'Area'
    ec_grouped_df = (
        df_filtered
        .groupby(["Area","Area Picture Link"], as_index=False)
        [['Sales', 'Sits', 'Opps']].sum()
        .sort_values('Sales', ascending=False)
    )

# Filter Field Marketer data based on date and area
fm_df_filtered = fm_df[(fm_df["Date"] >= start_date) & (fm_df["Date"] <= end_date)]
if area:
    fm_df_filtered = fm_df_filtered[fm_df_filtered["Area"].isin(area)]

# Group Field Marketer data
if dimension == 'Rep':
    fm_grouped_df = (
        fm_df_filtered
        .groupby(["FM", "FM Picture Link"], as_index=False)
        [['Assists', 'Sits', 'Sets']].sum()
        .sort_values('Assists', ascending=False)
    )
else:  # dimension == 'Area'
    fm_grouped_df = (
        fm_df_filtered
        .groupby(["Area", "Area Picture Link"], as_index=False)
        [['Assists', 'Sits', 'Sets']].sum()
        .sort_values('Assists', ascending=False)
    )

###############################################################################
# Create two tabs: Activity (driven by 'role') and Contract Value
###############################################################################
tab1, tab2 = st.tabs(["Activity", "Contract Value"])

with tab1:
    if role == 'Energy Consultant':
        # Dynamically determine the picture column based on the dimension
        picture_column = "Closer Picture Link" if dimension == 'Rep' else "Area Picture Link"

        # Define column order for display
        column_order = [
            picture_column,
            "Closer" if dimension == 'Rep' else "Area", 
            "Sales", 
            "Sits", 
            "Opps"
        ]

        st.data_editor(
            ec_grouped_df,
            column_config={picture_column: st.column_config.ImageColumn("")},
            column_order=column_order,
            hide_index=True,
            height=1000,
            disabled=True,
            use_container_width=True,
        )

    else:  # role == 'Field Marketer'
        # Dynamically determine the picture column based on the dimension
        picture_column = "FM Picture Link" if dimension == 'Rep' else "Area Picture Link"

        # Define column order
        column_order = [
            picture_column, 
            "FM" if dimension == 'Rep' else "Area", 
            "Assists", 
            "Sits", 
            "Sets"
        ]

        st.data_editor(
            fm_grouped_df,
            column_config={picture_column: st.column_config.ImageColumn("")},
            column_order=column_order,
            hide_index=True,
            height=1000,
            disabled=True,
            use_container_width=True,
        )

with tab2:
    st.data_editor(
        df_cv,
        hide_index=True,
        disabled=True,
        use_container_width=True,
        height=1000
    )