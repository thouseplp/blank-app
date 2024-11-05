import streamlit as st

def create_card(area, goal, actual, profile_image, name):
    # Calculate the percentage to goal, handling division by zero
    percentage_to_goal = min((actual / goal) * 100 if goal != 0 else 100, 100)
    # Set the progress bar color based on the percentage
    progress_color = "#C34547" if percentage_to_goal < 100 else "#32A077"

    st.markdown(f"""
        <div class="card">
            <div class="profile-section">
                <img src="{profile_image}" class="profile-pic" alt="Profile Picture">
                <div class="name">{name}</div>
            </div>
            <div class="appointments">{actual}</div>
            <div class="progress-bar">
                <div class="progress-bar-fill" style="width: {percentage_to_goal}%; background-color: {progress_color};"></div>
                <div class="goal">{goal}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)