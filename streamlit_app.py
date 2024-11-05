import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from transformation.data import process_appointments_data  # Import the function

st.set_page_config(
    page_title="Daily Set Goals",
    layout="wide"
)

st.logo("https://i.ibb.co/5R9N3Bs/DAILY-SET-GOALS.png", size="large")

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

date = st.date_input("Select your date")

df = process_appointments_data(date)

from features.progress_bar import create_card  # Import create_card from progress_bar

columns = st.columns(3)
for idx, row in df.iterrows():
    with columns[idx % 3]:
        create_card(
            area=row['AREA'],
            goal=row['GOALS'],
            actual=row['ID'],
            profile_image=row['PROFILE_PICTURE'],
            name=row['AREA']
        )