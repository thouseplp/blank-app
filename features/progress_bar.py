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

st.markdown("""
    <style>
    .card {
        background-color: #41434A;
        padding: 10px;
        border-radius: 10px;
        margin-bottom: 5px;
        color: white;
        position: relative;
    }
    .profile-section {
        display: flex;
        align-items: center;
        margin-bottom: 8px;
    }
    .profile-pic {
        border-radius: 50%;
        width: 28px;
        height: 28px;
        margin-right: 15px;
    }
    .name {
        font-size: 16px;
        font-weight: bold;
    }
    .appointments {
        font-size: 16px;
        margin-bottom: 10px;
        color: white;
    }
    .progress-bar {
        background-color: #37383C;
        border-radius: 25px;
        width: 100%;
        height: 20px;
        position: relative;
        margin-bottom: 10px;
    }
    .progress-bar-fill {
        background-color: #C34547;
        height: 100%;
        border-radius: 25px;
    }
    .goal {
        position: absolute;
        right: 5px;
        top: 50%;
        transform: translateY(-50%);
        font-size: 16px;
        color: white;
        font-weight: bold;
    }
    </style>
""", unsafe_allow_html=True)