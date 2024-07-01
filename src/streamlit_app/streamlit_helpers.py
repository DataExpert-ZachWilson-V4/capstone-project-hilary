import ast
import pandas as pd
import plotly.express as px
import streamlit as st
from trino.auth import BasicAuthentication
from trino.dbapi import connect


@st.cache_data(ttl=1200)
def execute_query(query, trino_creds):
    if isinstance(trino_creds, str):
        trino_creds = ast.literal_eval(trino_creds)
    conn = connect(
        host=trino_creds["TRINO_HOST"],
        port=int(trino_creds["TRINO_PORT"]),
        user=trino_creds["TRINO_USERNAME"],
        catalog=trino_creds["TRINO_CATALOG"],
        http_scheme="https",
        auth=BasicAuthentication(
            trino_creds["TRINO_USERNAME"], trino_creds["TRINO_PASSWORD"]
        ),
    )
    cur = conn.cursor()
    cur.execute(query)
    return pd.DataFrame(cur.fetchall(), columns=[col.name for col in cur.description])


def show_table(df):
    df.columns = [" ".join(c.split("_")).title() for c in df.columns]
    format_styles = {}
    for c in df.columns:
        if df[c].dtype == "float64":
            format_styles[c] = "{:,.2f}"
        elif df[c].dtype == "int64":
            format_styles[c] = "{:,}"
    st.table(
        df.style.format(format_styles).set_table_styles(
            {
                col: [
                    {
                        "selector": "th",
                        "props": [
                            ("background-color", "lightblue"),
                            ("color", "black"),
                            ("font-weight", "bold"),
                        ],
                    }
                ]
                for col in df.columns
            }
        )
    )
    hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """
    st.markdown(hide_table_row_index, unsafe_allow_html=True)


def create_delay_plot(
    df,
    x,
    y,
    color,
    title,
    x_title,
    y_title,
    hover_data,
    hover_template,
    width=800,
    height=800,
):
    df["flightdate"] = pd.to_datetime(df["flightdate"])
    df.sort_values("flightdate", inplace=True)
    fig = px.line(
        df,
        x=x,
        y=y,
        color=color,
        title=title,
        labels={x: x_title, y: y_title},
        hover_data=hover_data,
        template="plotly_white",
    )
    fig.update_traces(mode="markers+lines", hovertemplate=hover_template)
    fig.update_layout(
        autosize=False,
        width=width,
        height=height,
    )
    return fig


def create_market_share_plot(
    df, x, y, title, x_title, y_title, hover_data, hover_template, n=15
):
    fig = px.bar(
        df.head(n),
        x=x,
        y=y,
        title=title,
        labels={x: x_title, y: y_title},
        hover_data=hover_data,
        template="plotly_white",
    )
    fig.update_traces(marker_line_color="black", marker_line_width=1.5)
    fig.update_traces(hovertemplate=hover_template)
    return fig
