import streamlit as st
import numpy as np
import pandas as pd
import SessionState

from PIL import Image

from updateFunctions import *
from bigqueryClient import *

st.markdown(
    "# Boise State's Official and Definitive Ping Pong Rankings :crown: :trophy:"
)

# load up big query client
client = bigquery.Client()

# load up rankings
if "ranks" not in st.session_state:
    st.session_state["ranks"] = rankings(client)

if "names" not in st.session_state:
    st.session_state["names"] = st.session_state.ranks["name"].tolist()

if "table" not in st.session_state:
    ranks_disp = st.session_state.ranks.loc[st.session_state.ranks["games"] >= 10]
    ranks_disp.index = np.arange(1, len(ranks_disp) + 1)
    ranks_disp.index = np.arange(1, len(ranks_disp) + 1)
    st.session_state["table"] = ranks_disp.style.apply(
        lambda x: [
            "background: gold"
            if x.name == 1
            else (
                "background: silver"
                if x.name == 2
                else ("background: saddlebrown" if x.name == 3 else "")
            )
            for i in x
        ],
        axis=1,
    )

with st.container():
    table = st.table(st.session_state["table"])
    # _, _, _, col, _, _, _ = st.columns(7)
    # with col:
    #     st.button("Reload rankings")
    st.button("Reload rankings")

st.markdown("## Most Recent 5 Games :ledger:")
if "game_history_display" not in st.session_state:
    st.session_state["game_history_display"] = match_history_display(client)
st.write(st.session_state["game_history_display"])

st.sidebar.markdown("## Enter Match Results :black_nib:")
with st.sidebar.form(key="match_result", clear_on_submit=True):
    winner = st.selectbox("Winner: ", st.session_state.names)
    loser = st.selectbox("Loser: ", st.session_state.names[::-1])
    submitted = st.form_submit_button(label="Submit")
    # with st.spinner("Loading Model"):
    if submitted:
        if winner == loser:
            pass
        else:
            update_all(client, winner, loser, winner)
            del st.session_state["ranks"]
            del st.session_state["table"]
            del st.session_state["game_history_display"]

with st.sidebar.expander("Delete game"):
    delete_button = st.button("Delete last game entered")
    if delete_button:
        st.session_state["delete"] = True
    if "delete" in st.session_state:
        to_be_deleted = st.session_state["game_history_display"].iloc[0].tolist()
        st.error(
            f"Do you really, really, wanna delete game between {to_be_deleted[0]} and {to_be_deleted[1]}?"
        )
        if st.button("Yes, do it!"):
            go_back_all(client)
            del st.session_state["delete"]
            del st.session_state["ranks"]
            del st.session_state["table"]
            del st.session_state["game_history_display"]
        # if st.button(f"Are you sure you want to delete game between  and ?"):
        #     go_back_all(client)
        #     del st.session_state["ranks"]
        #     del st.session_state["table"]
        #     st.button("Reload rankings!")
# if st.sidebar.button("Update rankings"):
#     df = recordMatch(df, player1, player2, winner)
#     df = df.sort_values(by=["rating"], ascending=False, ignore_index=True)
#     toTweet = df.to_csv(sep=" ", index=True, header=True)
#     twitter.updateRankings(toTweet)
#     st.sidebar.button("Reload rankings!")
