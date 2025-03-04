import streamlit as st


pg_home = st.Page("pages/1_Home.py", title="Home", icon="🏠")
pg_inc_rail = st.Page(
    "pages/2_Incidents_Russian_Railways.py",
    title="Incidents Russian Railways",
    icon="🚂",
)

pg_block_site = st.Page(
    "pages/3_Blocked_Sites_in_Russia.py",
    title="Blocked Sites in Russia",
    icon="🚫",
)

pg = st.navigation([pg_home, pg_inc_rail, pg_block_site])

st.set_page_config(
    page_title="Ukraine-Russia Conflict Data Hub",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://www.extremelycoolapp.com/help",
        "Report a bug": "https://www.extremelycoolapp.com/bug",
        "About": "# This is a header. This is an *extremely* cool app!",
    },
)

pg.run()
