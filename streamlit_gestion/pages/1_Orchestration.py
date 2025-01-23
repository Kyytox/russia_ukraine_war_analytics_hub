import os
import sys
import json
import asyncio
import streamlit as st
import pandas as pd
import numpy as np


from prefect.client.orchestration import get_client
from prefect.server.schemas.actions import WorkPoolCreate
from prefect.deployments import run_deployment
from prefect.deployments.runner import RunnerDeployment
from prefect.runner.runner import Runner
from prefect.artifacts import Artifact

st.set_page_config(page_title="Orchestration", page_icon=":gear:", layout="wide")

# Get list of all Task Runs
# async def get_all_task_runs():
#     async with get_client() as client:
#         return await client.read_task_runs()


# st.write(asyncio.run(get_all_task_runs()))


# Get list of all flows
async def get_all_flows():
    client = get_client()
    return await client.read_deployments()


def ini_session_state():
    if "list_all_flows" not in st.session_state:
        st.session_state.list_all_flows = asyncio.run(get_all_flows())

    if "list_telegram_flows" not in st.session_state:
        tmp_ = init_list_flows("telegram", st.session_state.list_all_flows)
        st.session_state.list_telegram_flows = sort_flows(tmp_)

    if "list_twitter_flows" not in st.session_state:
        tmp_ = init_list_flows("twitter", st.session_state.list_all_flows)
        st.session_state.list_twitter_flows = sort_flows(tmp_)

    if "list_filter_flows" not in st.session_state:
        tmp_ = init_list_flows("filter", st.session_state.list_all_flows)
        st.session_state.list_filter_flows = sort_flows(tmp_)

    if "list_applicatifs_flows" not in st.session_state:
        tmp_ = init_list_flows("applicatifs", st.session_state.list_all_flows)
        st.session_state.list_applicatifs_flows = sort_flows(tmp_)

    if "list_flows_to_run" not in st.session_state:
        st.session_state.list_flows_to_run = []

    if "list_artifacts" not in st.session_state:
        st.session_state.list_artifacts = []


def init_list_flows(section, list_all_flows):
    """Generate list of flows

    Args:
        section (string): section of list to generate
        list_all_flows (list): list of all flows
    """

    if section == "telegram":
        list_flows = [
            flow
            for flow in list_all_flows
            if "telegram" in flow.name.lower() and "master" in flow.name.lower()
        ]
    elif section == "twitter":
        list_flows = [
            flow
            for flow in list_all_flows
            if "twitter" in flow.name.lower() and "master" in flow.name.lower()
        ]
    elif section == "filter":
        list_flows = [
            flow
            for flow in list_all_flows
            if ("filter" in flow.name.lower() or "pre" in flow.name.lower())
            and "master" in flow.name.lower()
        ]
    elif section == "applicatifs":
        list_flows = [
            flow
            for flow in list_all_flows
            if "applicatifs" in flow.name.lower() and "master" in flow.name.lower()
        ]

    # sort list
    list_flows = sort_flows(list_flows)

    return list_flows


def sort_flows(list_flows):
    """Sort list of flows

    Args:
        list_flows (list): list of flows

    Returns:
        list_flows: list of flows sorted
    """
    dic_order_flows = {
        "telegram-extract": 1,
        "telegram-clean": 2,
        "telegram-transform": 3,
        "twitter-extract": 4,
        "twitter-clean": 5,
        "filter": 6,
        "pre-classify": 7,
    }

    # sort according to the presence of substrings in dic_order_flows
    list_flows.sort(
        key=lambda x: next(
            (dic_order_flows[key] for key in dic_order_flows if key in x.name.lower()),
            0,
        )
    )

    return list_flows


def prepare_flows_to_run(flow, key_name):
    """Add flows to run to the list

    Args:
        flow: flow object

    Returns:
        session_state: list_flows_to_run
    """

    # Add flow to run
    if st.session_state[key_name]:
        if flow not in st.session_state.list_flows_to_run:
            st.session_state.list_flows_to_run.append(flow)
    else:
        if flow in st.session_state.list_flows_to_run:
            st.session_state.list_flows_to_run.remove(flow)

    # Sort list_flows_to_run
    st.session_state.list_flows_to_run = sort_flows(st.session_state.list_flows_to_run)


##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################

ini_session_state()


async def call(list_flows):

    if len(list_flows) == 0:
        st.error("No flows to run")
        return

    with st.status("Execute Pipeline...", expanded=True) as status:

        for flow in list_flows:
            res = await run_deployment(flow.id)

            if res.state_type == "COMPLETED":
                st.write(f"Flow {flow.name} completed ✅")

                # Get artifacts
                flow_artifact = await Artifact.get(f"flow-{flow.name}-artifact")
                st.session_state.list_artifacts.append(flow_artifact)

            else:
                status.update(label="Pipeline failed!", state="error", expanded=True)
                st.write(res)
                return

        status.update(label="Pipeline completed!", state="complete", expanded=True)

    st.session_state.list_flows_to_run = []

    for key in st.session_state.keys():
        if "key_" in key:
            st.session_state[key] = False

    st.success("All flows completed")
    if st.button("refresh"):
        st.rerun()


with st.sidebar:

    if st.session_state.list_flows_to_run:
        if st.button("Run Flows", type="primary"):
            asyncio.run(call(st.session_state.list_flows_to_run))

        st.subheader("Flows to Run")
        for flow in st.session_state.list_flows_to_run:
            st.code(flow.name)
    else:
        st.info("Select flows to run")


st.divider()

col1, col2, col3, col4 = st.columns(4, border=True)

with col1:
    st.subheader("Telegram Flows")
    for flow in st.session_state.list_telegram_flows:
        key_name = f'key_{flow.name.replace(" ", "_").lower()}'
        st.toggle(
            flow.name.replace("master", "").replace("-", " ").title(),
            key=key_name,
            on_change=prepare_flows_to_run,
            args=(flow, key_name),
        )

with col2:
    st.subheader("Twitter Flows")
    for flow in st.session_state.list_twitter_flows:
        key_name = f'key_{flow.name.replace(" ", "_").lower()}'
        st.toggle(
            flow.name.replace("master", "").replace("-", " ").title(),
            key=key_name,
            on_change=prepare_flows_to_run,
            args=(flow, key_name),
        )


with col3:
    st.subheader("Filter Flows")
    for flow in st.session_state.list_filter_flows:
        key_name = f'key_{flow.name.replace(" ", "_").lower()}'
        st.toggle(
            flow.name.replace("master", "").replace("-", " ").title(),
            key=key_name,
            on_change=prepare_flows_to_run,
            args=(flow, key_name),
        )

# with col4:
#     st.subheader("Applicatifs Flows")
#     # for flow in list_flows:
#     #     st.write(flow.name)


# st.write(st.session_state.list_flows_to_run)

st.divider()

# st.write(st.session_state.list_artifacts)
if st.session_state.list_artifacts:
    st.subheader("Artifacts")
    for artifact in st.session_state.list_artifacts:
        st.write(artifact.key)
        st.write(pd.DataFrame(json.loads(artifact.data)))
