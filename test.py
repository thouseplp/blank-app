import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from transformation.data import process_appointments_data  # Import the function
from features.progress_bar import create_card, sales_target

sales_target(
    actual=2,
    pace=12,
    minimum_target=12,
    maximum_target=15,
    image="https://res.cloudinary.com/dwuzrptk6/image/upload/v1730863714/bend_dvre85.png",
    area="Bend"
)