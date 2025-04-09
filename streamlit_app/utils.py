import json
import shutil
import pathlib

from bs4 import BeautifulSoup

import streamlit as st
import streamlit.components.v1 as components
from streamlit_extras.tags import tagger_component


def init_css():
    st.markdown(
        """
        <style>
        
        .stMainBlockContainer {
            padding-left: 2rem;
            padding-right: 2rem;
        }
        
        .st-emotion-cache-1jicfl2 {

            padding-bottom: 3rem;
            padding-top: 3rem;
        }     
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px;
        }

        .stTabs [data-baseweb="tab"] {
            height: 35px;
            white-space: pre-wrap;
            background-color: #979797;
            border-radius: 4px 4px 0px 0px;
            padding: 0px 10px;
            color: #000000;
        }
        
        .stTabs [data-baseweb="tab"]:nth-child(8) {
            background-color: #919191;
        }

        .stTabs [aria-selected="true"] {
            background-color: #FFFFFF;
            
        }
        
        footer:after {
            content:'goodbye'; 
            visibility: visible;
            display: block;
            position: relative;
            #background-color: red;
            padding: 5px;
            top: 2px;
        }


        /* sidebar box (develop by + avatar) */
        .stSidebar [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] {
            position: fixed;
            bottom: 0;
        }
        .stSidebar [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] [data-testid="element-container"]:first-child {
            width: 40px;
        }
        
        /* use this code inside your css file */
        div.stMetric{
            background-color: #1a212e;
            border: 2px solid;
            padding: 20px 20px 20px 20px;
            border-radius: 10px;
            color: #929292;
            box-shadow: 10px;
            height: 8rem;
        }
        div.stMetric p{
            color: #ffffff;
            font-size: 1rem;
            white-space: wrap;
        }
        div.stMetric [data-testid="stMetricValue"]{
            color: #ffffff;
            font-weight: 700;
        }

        /* -- image -- */
        img {
            border-radius: 3px;
        }
        
        
        /* -- Button page link -- */
        
        .stLinkButton {
            align-items: center;
            display: flex;
            justify-content: center;
        }
        
            
        [data-testid="stPageLink-NavLink"], .stLinkButton a {
            padding-left: 23px;
            padding-right: 23px;
            padding-bottom: 10px;
            padding-top: 10px;
            margin-top: 0.7rem;
            margin-bottom: 1rem;
            border-radius: 9px;
            background: #0057B8;
            border: none;
            font-family: inherit;
            text-align: center;
            cursor: pointer;
            transition: 0.4s;
            text-decoration: none;
            color: #ffffff;
        }

        [data-testid="stPageLink-NavLink"]:hover, .stLinkButton a:hover {
            background: #0057B8;
            box-shadow: 7px 10px 70px -14px #FFD700;
            color: #ffffff;
        }

        [data-testid="stPageLink-NavLink"]:active, .stLinkButton a:active {
            transform: scale(0.97);
            box-shadow: 7px 10px 70px -10px #FFD700;
            color: #ffffff;
        }
        
        
        /* -- tags streamlit-extras -- */
        .stColumn [data-testid="stMarkdownContainer"] p {
            display: flex;
            justify-content: center;
        }

        [data-testid="stMarkdownContainer"] p span {
            border-radius: 5px !important;
        }
                
        </style>""",
        unsafe_allow_html=True,
    )


def add_analytics_tag():
    # replace G-XXXXXXXXXX to your web app's ID
    analytics_js = """
    <!-- Global site tag (gtag.js) - Google Analytics -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-PSVZK81FYS"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){dataLayer.push(arguments);}
        gtag('js', new Date());
        gtag('config', 'G-PSVZK81FYS');
    </script>
    <div id="G-PSVZK81FYS"></div>
    """
    analytics_id = "G-PSVZK81FYS"

    # Identify html path of streamlit
    index_path = pathlib.Path(st.__file__).parent / "static" / "index.html"
    soup = BeautifulSoup(index_path.read_text(), features="html.parser")
    if not soup.find(id=analytics_id):  # if id not found within html file
        bck_index = index_path.with_suffix(".bck")
        if bck_index.exists():
            shutil.copy(bck_index, index_path)  # backup recovery
        else:
            shutil.copy(index_path, bck_index)  # save backup
        html = str(soup)
        new_html = html.replace("<head>", "<head>\n" + analytics_js)
        index_path.write_text(new_html)


def developper_link():
    """
    Add a link to the github repository
    """

    with st.container():
        components.html(
            """
            <div style="display: flex; align-items: center; justify-content: center;">
                <a href="https://x.com/Kytox_" target="_blank">
                    <img src="https://pbs.twimg.com/profile_images/1471129038022455299/Zn05GePO_400x400.jpg" style="width: 30px; border-radius: 50%;">
                </a>
                <a href="https://x.com/Kytox_" target="_blank" style="margin-left: 10px; font-size: 0.9rem; color: #ffffff; text-decoration: none; width: 100%;">
                    Developed by: Kytox
                </a>
            </div>
            """,
            height=50,
            width=200,
        )


def jump_lines(z=2):
    n = 0
    while n < z:
        st.write("")
        n += 1


def get_region():
    """
    Get the region and id from the json file
    """

    # read file json
    with open("core/utils/ru_region.json") as file:
        data = json.load(file)

    # get id and name
    dict_region = {}
    for i in range(len(data["features"])):
        dict_region[data["features"][i]["properties"]["name"]] = data["features"][i][
            "id"
        ]

    # update dict
    dict_region = {
        k.replace("Moskva", "Moscow").replace("'", ""): v
        for k, v in dict_region.items()
    }

    return dict_region


def display_container_content(dict_data: dict):
    """
    Display the container content

    Args:
        dict_data: dictionary containing the data to display
    """

    if dict_data[list(dict_data.keys())[0]]["image"]:
        col1, col2, col3 = st.columns([1, 0.1, 0.1])
    else:
        col1, col2 = st.columns([1, 0.3])

    for key, value in dict_data.items():
        with col1:
            with st.container(border=True):

                if value["image"]:

                    subcol1, subcol2, subcol3 = st.columns(
                        [0.3, 0.4, 0.3], vertical_alignment="center", gap="small"
                    )

                    with subcol1:
                        st.image(value["image"], use_container_width=True)
                    with subcol2:
                        html_code = (
                            f"<h2 style='text-align: center;'>{value['title']}</h2>"
                        )
                        st.markdown(html_code, unsafe_allow_html=True)

                        html_code = (
                            f"<p style='text-align: center;'>{value['text']}</p>"
                        )
                        st.markdown(html_code, unsafe_allow_html=True)

                        tagger_component(
                            "", value["tags"], color_name=value["color_tags"]
                        )

                    with subcol3:
                        st.page_link(value["url"], label=f"{value['title']} ->")

                else:

                    subcol1, subcol2 = st.columns(
                        [0.7, 0.3], vertical_alignment="center", gap="small"
                    )

                    with subcol1:
                        html_code = (
                            f"<h2 style='text-align: center;'>{value['title']}</h2>"
                        )
                        st.markdown(html_code, unsafe_allow_html=True)

                        html_code = (
                            f"<p style='text-align: center;'>{value['text']}</p>"
                        )
                        st.markdown(html_code, unsafe_allow_html=True)

                        tagger_component(
                            "", value["tags"], color_name=value["color_tags"]
                        )

                    with subcol2:
                        st.link_button(label=f"{value['title']} ->", url=value["url"])

            jump_lines(2)
