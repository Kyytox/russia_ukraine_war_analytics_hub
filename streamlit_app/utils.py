import json
import shutil
import pathlib

from bs4 import BeautifulSoup

import streamlit as st
import streamlit.components.v1 as components


def init_css():
    st.markdown(
        """<style>
        
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

                
        </style>""",
        unsafe_allow_html=True,
    )


def add_analytics_tag():
    # replace G-XXXXXXXXXX to your web app's ID

    analytics_js = """
    <!-- Global site tag (gtag.js) - Google Analytics -->
    <script async src="https://www.googletagmanager.com/gtag/js?id=G-GKBT8QLJ3W"></script>
    <script>
        window.dataLayer = window.dataLayer || [];
        function gtag(){dataLayer.push(arguments);}
        gtag('js', new Date());
        gtag('config', 'G-GKBT8QLJ3W');
    </script>
    <div id="G-GKBT8QLJ3W"></div>
    """
    analytics_id = "G-GKBT8QLJ3W"

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
                <a href="https://x.com/kytox__" target="_blank">
                    <img src="https://pbs.twimg.com/profile_images/1471129038022455299/Zn05GePO_400x400.jpg" style="width: 30px; border-radius: 50%;">
                </a>
                <a href="https://x.com/kytox__" target="_blank" style="margin-left: 10px; font-size: 0.9rem; color: #ffffff; text-decoration: none; width: 100%;">
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
