import streamlit as st
import datetime
from transformation.leaderboard import leaderboard_query, fm_leaderboard_query

# Set up Streamlit page configuration
st.set_page_config(
    page_title="Leaderboards",
    layout="wide"
)

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

df = leaderboard_query()

fm_df = fm_leaderboard_query()

unique_areas = sorted(df["Area"].unique())

today = datetime.date.today()

if today.month == 12:
    next_month = datetime.date(today.year + 1, 1, 1)
else:
    next_month = datetime.date(today.year, today.month + 1, 1)

end_of_month = next_month - datetime.timedelta(days=1)
end_of_month_day = end_of_month.day

cols1, cols2, cols3 = st.columns([1,1,1])

with cols1:
    date_range = st.date_input("Date", (datetime.date(today.year, today.month, 1), datetime.date(today.year, today.month, end_of_month_day)))
    start_date, end_date = date_range

with cols2:
    area = st.multiselect("Area", unique_areas)

with cols3:
    dimension = st.selectbox("Dimension", ('Rep', 'Area'))

df_filtered = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

if area:
    df_filtered = df_filtered[df_filtered["Area"].isin(area)]

if dimension == 'Rep':
    grouped_df = df_filtered.groupby(["Closer","Closer Picture Link"], as_index=False)[['Sales', 'Sits', 'Opps']].sum().sort_values('Sales', ascending=False)
elif dimension == 'Area':
    grouped_df = df_filtered.groupby(["Area", "Area Picture Link"], as_index=False)[['Sales', 'Sits', 'Opps']].sum().sort_values('Sales', ascending=False)

tab1, tab2 = st.tabs(["Energy Consultant", "Field Marketer"])

with tab1:
    # Dynamically determine the picture column based on the dimension
    picture_column = "Closer Picture Link" if dimension == 'Rep' else "Area Picture Link"

    column_order = [picture_column, "Closer" if dimension == 'Rep' else "Area", "Sales", "Sits", "Opps"]

    # Display the data_editor with the appropriate image column
    st.data_editor(
        grouped_df,
        column_config={picture_column: st.column_config.ImageColumn("")},
        column_order=column_order,
        hide_index=True,
        height=1000,
        disabled=True,
        use_container_width=True,
    )

fm_df_filtered = fm_df[(fm_df["Date"] >= start_date) & (fm_df["Date"] <= end_date)]

if area:
    fm_df_filtered = fm_df_filtered[fm_df_filtered["Area"].isin(area)]

if dimension == 'Rep':
    fm_grouped_df = fm_df_filtered.groupby(["FM", "FM Picture Link"], as_index=False)[['Assists', 'Sits', 'Sets']].sum().sort_values('Assists', ascending=False)
elif dimension == 'Area':
    fm_grouped_df = fm_df_filtered.groupby(["Area", "Area Picture Link"], as_index=False)[['Assists', 'Sits', 'Sets']].sum().sort_values('Assists', ascending=False)

with tab2:
    # Dynamically determine the picture column based on the dimension
    picture_column = "FM Picture Link" if dimension == 'Rep' else "Area Picture Link"

    # Add the picture column to fm_grouped_df (if applicable in your data)
    # Ensure your fm_df contains the corresponding picture link columns

    # Define the column order
    column_order = [picture_column, "FM" if dimension == 'Rep' else "Area", "Assists", "Sits", "Sets"]

    # Display the data_editor with the appropriate image column
    st.data_editor(
        fm_grouped_df,
        column_config={picture_column: st.column_config.ImageColumn("")},
        column_order=column_order,
        hide_index=True,
        height=1000,
        disabled=True,
        use_container_width=True,
    )