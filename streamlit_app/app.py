import streamlit as st

from utils import (
    add_analytics_tag,
)


# Google Analytics
add_analytics_tag()

pg_home = st.Page("pages/1_Home.py", title="Home", icon="🏠")
pg_inc_rail = st.Page(
    "pages/2_Incidents_Russian_Railways.py",
    title="Incidents Russian Railways",
    icon="🚂",
)

pg_block_site = st.Page(
    "pages/3_Blocked_Websites_in_Russia.py",
    title="Blocked Sites in Russia",
    icon="🚫",
)

pg_raid_alerts = st.Page(
    "pages/4_Raid_Alerts_Ukraine.py",
    title="Raid Alerts in Ukraine",
    icon="🚨",
)

pg_compo_weapons = st.Page(
    "pages/5_Components_Weapons.py",
    title="Components Weapons",
    icon="⚙️",
)

pg = st.navigation(
    [pg_home, pg_inc_rail, pg_block_site, pg_raid_alerts, pg_compo_weapons]
)

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
