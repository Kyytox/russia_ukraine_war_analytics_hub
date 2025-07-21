import time
import json
import asyncio
import streamlit as st
import pandas as pd

from prefect.runner.runner import Runner
from prefect.deployments.runner import RunnerDeployment
from prefect.server.schemas.actions import WorkPoolCreate

from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment
from prefect.artifacts import Artifact

st.set_page_config(page_title="Orchestration", page_icon=":gear:", layout="wide")


list_tags_sort = ["telegram", "twitter", "filter", "qualification", "dwh", "classify"]
list_flows_sort = ["extract", "clean", "transform", "ingest", "dmt"]


# Get list of all flows
async def get_all_flows():
    client = get_client()
    list_flows = await client.read_deployments()

    # Create a df to store flows
    df_flows = pd.DataFrame([flow.dict() for flow in list_flows])
    df_flows = df_flows[["id", "name", "tags", "entrypoint", "created"]]

    return df_flows


def ini_session_state():
    if "list_all_flows" not in st.session_state:
        st.session_state.list_all_flows = asyncio.run(get_all_flows())

    if "list_flows_to_run" not in st.session_state:
        st.session_state.list_flows_to_run = []

    if "list_artifacts" not in st.session_state:
        st.session_state.list_artifacts = []

    if "nb_loop" not in st.session_state:
        st.session_state["nb_loop"] = 1


def sort_list_by_priority(list_to_sort, list_ref):
    """Sort tags based on priority order

    Args:
        list_to_sort (list): list of tags to sort
        list_ref (list): reference list for sorting

    Returns:
        list: sorted tags list
    """

    def get_tag_priority(tag):

        if type(list_to_sort[0]) is not str:
            value_lower = tag["name"].lower()
        else:
            value_lower = tag.lower()

        for i, keyword in enumerate(list_ref):
            if keyword in value_lower:
                return i
        return len(list_ref)  # Return a high value if no keyword matches

    # Sort the list using the priority function
    sorted_list = sorted(list_to_sort, key=get_tag_priority)
    return sorted_list


def prepare_flows_to_run(flow, key_name):
    """Add flows to run to the list

    Args:
        flow: flow object
        key_name (str): key name for the toggle button
    """

    # Add flow to run
    if st.session_state[key_name]:
        if flow["name"] not in st.session_state.list_flows_to_run:
            st.session_state.list_flows_to_run.append(flow["name"])
    else:
        if flow["name"] in st.session_state.list_flows_to_run:
            st.session_state.list_flows_to_run.remove(flow["name"])

    # Sort list_flows_to_run
    st.session_state.list_flows_to_run = sort_list_by_priority(
        st.session_state.list_flows_to_run, list_flows_sort
    )


async def call(list_flows, step):
    """
    Run flows

    Args:
        list_flows (list): list of flows to run
        step (int): step number
    """
    if len(list_flows) == 0:
        st.error("No flows to run")
        return

    # create temp df with infos of flows to run
    df_flows_to_run = pd.DataFrame(list_flows, columns=["name"])
    df_flows_to_run = df_flows_to_run.merge(
        st.session_state.list_all_flows, on="name", how="left"
    )

    with st.status("Execute Pipeline...", expanded=True) as status:

        for flow in df_flows_to_run.itertuples(index=False):
            res = await run_deployment(flow.id)

            if res.state_type == "COMPLETED":
                st.write(f"Flow {flow.name} completed ✅")

                # Get artifacts
                flow_artifact = await Artifact.get(f"{flow.name}-artifact")
                st.session_state.list_artifacts.append(flow_artifact)

            else:
                status.update(label="Pipeline failed!", state="error", expanded=True)
                st.write(res)
                return

        status.update(label="Pipeline completed!", state="complete", expanded=True)
    if step == 0 or step == st.session_state.nb_loop:
        st.success("All flows completed")
        if st.button("refresh"):

            # put all sesion state toggle_key to false
            for key in st.session_state.keys():
                if key.startswith("toggle_key_"):
                    st.session_state[key] = False

            # empty list_flows_to_run
            st.session_state.list_flows_to_run = []

            st.rerun()
    else:
        st.divider()


# ##############################################################################################
# ##############################################################################################
# ##############################################################################################
# ##############################################################################################

ini_session_state()


with st.sidebar:

    if st.session_state.list_flows_to_run:

        # Select loop
        st.session_state["nb_loop"] = st.number_input(
            "Number of loops", min_value=1, max_value=10, value=1, step=1
        )

        if st.button("Run Flows", type="primary"):

            if st.session_state.nb_loop > 1:
                for step in range(1, st.session_state.nb_loop + 1):
                    asyncio.run(call(st.session_state.list_flows_to_run, step))
                    time.sleep(130)
            else:
                asyncio.run(call(st.session_state.list_flows_to_run, 0))

        st.subheader("Flows to Run")
        for flow in st.session_state.list_flows_to_run:
            st.code(flow)
    else:
        st.info("Select flows to run")


# Group flows by tags
flows_by_tag = {}
for _, flow in st.session_state.list_all_flows.iterrows():
    # Handle cases where tags might be a list or string
    if isinstance(flow["tags"], list):
        tags = flow["tags"]
    else:
        # If tags is a string representation of a list or single tag
        tags = [flow["tags"]] if flow["tags"] else []

    for tag in tags:
        if tag not in flows_by_tag:
            flows_by_tag[tag] = []
        flows_by_tag[tag].append(flow)

# Get unique tags for column organization
unique_tags = list(flows_by_tag.keys())
unique_tags = sort_list_by_priority(unique_tags, list_tags_sort)
nb_cols = len(unique_tags)


# Determine row number based on the number of cols (Warning number od cols by row is fixed to 4)
max_cols_per_row = 4
nb_rows = nb_cols // max_cols_per_row
if nb_cols % max_cols_per_row != 0:
    nb_rows += 1


# Display flows in a grid format organized by tags
for row in range(nb_rows):
    cols = st.columns(max_cols_per_row, border=True)

    for col in range(max_cols_per_row):
        index = row * max_cols_per_row + col
        if index < nb_cols:
            tag = unique_tags[index]
            flows_with_this_tag = flows_by_tag[tag]

            if not flows_with_this_tag:
                continue

            # Sort flows by priority
            flows_with_this_tag = sort_list_by_priority(
                flows_with_this_tag, list_flows_sort
            )

            with cols[col]:
                st.subheader(f"{tag.title()} Flows")

                # Display all flows with this tag
                for flow in flows_with_this_tag:
                    key_name = f'toggle_key_{flow["name"].replace(" ", "_").lower()}'
                    st.toggle(
                        flow["name"].replace("master", "").replace("-", " ").title(),
                        key=key_name,
                        on_change=prepare_flows_to_run,
                        args=(flow, key_name),
                    )


st.divider()

if st.session_state.list_artifacts:
    st.subheader("Artifacts")
    df = pd.DataFrame()

    for artifact in st.session_state.list_artifacts:
        df_ = pd.DataFrame(json.loads(artifact.data))
        df_["flow_name"] = artifact.key
        df = pd.concat([df, df_], ignore_index=True)

    st.dataframe(df, width=800, height=1000)
