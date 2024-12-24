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

###############################################################################
# Pull data from queries
###############################################################################
# 1) Energy Consultant data
df = leaderboard_query()

# 2) Field Marketer data
fm_df = fm_leaderboard_query()

# 3) Contract Value data
#    NOTE: We expect df_cv to have:
#      - "Sale Date" as the date column
#      - "Area"
#      - "Closer" or "Field Marketer" (depending on role)
#      - Possibly "Closer Picture Link" or "FM Picture Link" if you have images
df_cv = cv_query()

###############################################################################
# Get unique areas (you could also pull from df_cv if you prefer)
###############################################################################
unique_areas = sorted(df["Area"].unique())

###############################################################################
# Calculate default date range (first of current month - last day of current month)
###############################################################################
today = datetime.date.today()
if today.month == 12:
    next_month = datetime.date(today.year + 1, 1, 1)
else:
    next_month = datetime.date(today.year, today.month + 1, 1)
end_of_month = next_month - datetime.timedelta(days=1)
end_of_month_day = end_of_month.day

###############################################################################
# Sidebar-like controls
###############################################################################
cols1, cols2, cols3, cols4 = st.columns([1, 1, 1, 1])

with cols1:
    date_range = st.date_input(
        "Date",
        (
            datetime.date(today.year, today.month, 1),
            datetime.date(today.year, today.month, end_of_month_day),
        )
    )
    start_date, end_date = date_range

with cols2:
    area = st.multiselect("Area", unique_areas)

with cols3:
    dimension = st.selectbox("Dimension", ("Rep", "Area"))

with cols4:
    role = st.selectbox("Role", ("Energy Consultant", "Field Marketer"))

###############################################################################
# 1) Filter & group ENERGY CONSULTANT data (df)
###############################################################################
df_filtered = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

if area:
    df_filtered = df_filtered[df_filtered["Area"].isin(area)]

# Group by dimension
if dimension == "Rep":
    ec_grouped_df = (
        df_filtered
        .groupby(["Closer", "Closer Picture Link"], as_index=False)
        [["Sales", "Sits", "Opps"]]
        .sum()
        .sort_values("Sales", ascending=False)
    )
    ec_picture_col = "Closer Picture Link"
    ec_name_col = "Closer"
else:  # dimension == "Area"
    ec_grouped_df = (
        df_filtered
        .groupby(["Area", "Area Picture Link"], as_index=False)
        [["Sales", "Sits", "Opps"]]
        .sum()
        .sort_values("Sales", ascending=False)
    )
    ec_picture_col = "Area Picture Link"
    ec_name_col = "Area"

###############################################################################
# 2) Filter & group FIELD MARKETER data (fm_df)
###############################################################################
fm_df_filtered = fm_df[(fm_df["Date"] >= start_date) & (fm_df["Date"] <= end_date)]

if area:
    fm_df_filtered = fm_df_filtered[fm_df_filtered["Area"].isin(area)]

if dimension == "Rep":
    fm_grouped_df = (
        fm_df_filtered
        .groupby(["FM", "FM Picture Link"], as_index=False)
        [["Assists", "Sits", "Sets"]]
        .sum()
        .sort_values("Assists", ascending=False)
    )
    fm_picture_col = "FM Picture Link"
    fm_name_col = "FM"
else:  # dimension == "Area"
    fm_grouped_df = (
        fm_df_filtered
        .groupby(["Area", "Area Picture Link"], as_index=False)
        [["Assists", "Sits", "Sets"]]
        .sum()
        .sort_values("Assists", ascending=False)
    )
    fm_picture_col = "Area Picture Link"
    fm_name_col = "Area"

###############################################################################
# 3) Filter & group CONTRACT VALUE data (df_cv)
###############################################################################
# 3a) Filter by date range using "Sale Date"
df_cv_filtered = df_cv[
    (df_cv["Sale Date"] >= start_date) & (df_cv["Sale Date"] <= end_date)
]

# 3b) Filter by area if selected
if area:
    df_cv_filtered = df_cv_filtered[df_cv_filtered["Area"].isin(area)]

# 3c) Decide how to group the CV data based on dimension and role
if dimension == "Rep":
    # If the user picks 'Energy Consultant', group by "Closer"
    # If the user picks 'Field Marketer', group by "Field Marketer"
    if role == "Energy Consultant":
        cv_grouped_df = (
            df_cv_filtered
            .groupby(["Closer", "Closer Picture Link"], as_index=False)
            [["CV", "Solar", "Batteries", "Roofs", "Bundled"]]
            .sum()
            .sort_values("CV", ascending=False)
        )
        cv_picture_col = "Closer Picture Link"
        cv_name_col = "Closer"
    else:  # role == "Field Marketer"
        cv_grouped_df = (
            df_cv_filtered
            .groupby(["Field Marketer", "FM Picture Link"], as_index=False)
            [["CV", "Solar", "Batteries", "Roofs", "Bundled"]]
            .sum()
            .sort_values("CV", ascending=False)
        )
        cv_picture_col = "FM Picture Link"
        cv_name_col = "Field Marketer"

else:  # dimension == "Area"
    # NOTE: If your df_cv does NOT have "Area Picture Link", remove it from groupby
    #       We'll remove it here to avoid KeyErrors.
    cv_grouped_df = (
        df_cv_filtered
        .groupby("Area", as_index=False)
        [["CV", "Solar", "Batteries", "Roofs", "Bundled"]]
        .sum()
        .sort_values("CV", ascending=False)
    )
    cv_picture_col = None  # We won't display an image for area
    cv_name_col = "Area"

# Round CV if desired
cv_grouped_df["CV"] = cv_grouped_df["CV"].round(0)

###############################################################################
# Create two tabs: "Activity" (driven by 'role') and "Contract Value"
###############################################################################
tab1, tab2 = st.tabs(["Activity", "Contract Value"])

###############################################################################
# TAB 1 (Activity)
###############################################################################
with tab1:
    if role == "Energy Consultant":
        st.data_editor(
            ec_grouped_df,
            column_config={ec_picture_col: st.column_config.ImageColumn("")}
            if ec_picture_col in ec_grouped_df.columns
            else None,
            column_order=[
                col for col in (
                    ec_picture_col,
                    ec_name_col,
                    "Sales",
                    "Sits",
                    "Opps"
                ) if col in ec_grouped_df.columns
            ],
            hide_index=True,
            height=1000,
            disabled=True,
            use_container_width=True,
        )
    else:  # Field Marketer
        st.data_editor(
            fm_grouped_df,
            column_config={fm_picture_col: st.column_config.ImageColumn("")}
            if fm_picture_col in fm_grouped_df.columns
            else None,
            column_order=[
                col for col in (
                    fm_picture_col,
                    fm_name_col,
                    "Assists",
                    "Sits",
                    "Sets"
                ) if col in fm_grouped_df.columns
            ],
            hide_index=True,
            height=1000,
            disabled=True,
            use_container_width=True,
        )

###############################################################################
# TAB 2 (Contract Value)
###############################################################################
with tab2:
    # If we have a picture column for CV (e.g. Closer Picture Link or FM Picture Link)
    if cv_picture_col and cv_picture_col in cv_grouped_df.columns:
        st.data_editor(
            cv_grouped_df,
            column_config={cv_picture_col: st.column_config.ImageColumn("")},
            column_order=[
                cv_picture_col,
                cv_name_col,
                "CV",
                "Solar",
                "Batteries",
                "Roofs",
                "Bundled",
            ],
            hide_index=True,
            disabled=True,
            use_container_width=True,
            height=1000,
        )
    else:
        # No valid picture column, so skip that in the display
        st.data_editor(
            cv_grouped_df,
            column_order=[
                cv_name_col,
                "CV",
                "Solar",
                "Batteries",
                "Roofs",
                "Bundled",
            ],
            hide_index=True,
            disabled=True,
            use_container_width=True,
            height=1000,
        )