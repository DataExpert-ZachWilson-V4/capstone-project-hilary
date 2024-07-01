import os
import pandas as pd
import streamlit as st

from streamlit_helpers import (
    create_delay_plot,
    create_market_share_plot,
    execute_query,
    show_table,
)

trino_creds = os.getenv("TRINO_CREDENTIALS")
st.set_page_config(layout="wide")
fake_today = (pd.Timestamp.now() - pd.DateOffset(years=1)).strftime("%Y-%m-%d")
st.write(f"# Reporting Airline and Airport Metrics")
st.write(
    f"{fake_today} (Going back in time one year due to data availability. Simulated as incoming data.)"
)

airport_delay_query = f"""
with
    interval_data as (
        select *
        from sarneski44638.airport_airline_daily
        where
            flightdate between date('{fake_today}') - interval '6' day and date('{fake_today}')
    ),
    not_small_airports as (
        select airport_id
        from interval_data
        group by airport_id
        having avg(n_flights) > 50
    )
select
    d.airport_id,
    a.airport_abbrev as airport_abbreviation,
    a.airport_location,
    flightdate,
    sum(n_dep_delay) as n_flight_departure_delays,
    sum(n_arr_delay) as n_flight_arrival_delays,
    sum(n_flights) as n_flights,
    100 * sum(n_arr_delay) / sum(n_flights) as pct_arrival_delay,
    100 * sum(n_dep_delay) / sum(n_flights) as pct_departure_delay
from
    interval_data d
    join not_small_airports ns on ns.airport_id = d.airport_id
    join sarneski44638.dim_bts_airport a 
      on cast(d.airport_id as int) = a.airport_id
group by
    d.airport_id,
    a.airport_abbrev,
    a.airport_location,
    flightdate
"""
airport_delay_df = execute_query(airport_delay_query, trino_creds)

# Create for delays by airport over time
hover_data = [
    "airport_location",
    "n_flights",
    "airport_abbreviation",
    "pct_departure_delay",
    "n_flight_departure_delays",
    "n_flight_arrival_delays",
]
hovertemplate = (
    "Airport %{customdata[2]}<br>"
    + "Location: %{customdata[0]}<br>"
    + "Number of flights: %{customdata[1]}<br>"
    + "Flight Date: %{x} <br>"
    + "%{y}% of flights have arrival delays (%{customdata[5]} flights)<br>"
    + "%{customdata[3]}% of flights have departure delays (%{customdata[4]} flights)<br>"
    + "<extra></extra>"
)
fig = create_delay_plot(
    airport_delay_df,
    x="flightdate",
    y="pct_arrival_delay",
    color="airport_abbreviation",
    title="Percentage of Flights with Arrival Delays over Time (Large Airports)",
    x_title="Flight Date",
    y_title="% Flights with Arrival Delays",
    hover_data=hover_data,
    hover_template=hovertemplate,
)
st.plotly_chart(fig, use_container_width=True)

# Cancellations by airport and airline
n = 10
cancel_by_airline_query = f"""
with
    cancelled_by_airline as (
        select reporting_airline,
        sum(n_cancelled) as total_cancelled_flights,
        sum(n_flights) as total_scheduled_flights,
        sum(n_seats) as total_seats,
        dense_rank() over (
            order by
                sum(n_cancelled) desc
        ) as rnk
        from
            sarneski44638.airport_airline_daily
        where
            flightdate between date('{fake_today}') - interval '6' day and date('{fake_today}')
        group by
            reporting_airline
    )
select
    reporting_airline,
    total_cancelled_flights,
    total_scheduled_flights,
    total_seats
from
    cancelled_by_airline
where
    rnk <= {n}
order by
    rnk asc
"""
cancel_by_airline_df = execute_query(cancel_by_airline_query, trino_creds)

cancel_by_airport_query = f"""
with
    cancelled_by_airport as (
        SELECT
            airport_id,
            sum(n_cancelled) as total_cancelled_flights,
            sum(n_flights) as total_scheduled_flights,
            sum(n_seats) as total_seats,
            dense_rank() over (
                order by
                    sum(n_cancelled) desc
            ) as rnk
        from
            sarneski44638.airport_airline_daily
        where
            flightdate between date('{fake_today}') - interval '6' day and date('{fake_today}')
        group by
            airport_id
    )
select
    c.airport_id,
    a.airport_abbrev as airport_abbreviation,
    a.airport_location,
    total_cancelled_flights,
    total_scheduled_flights,
    total_seats
from
    cancelled_by_airport c
    join sarneski44638.dim_bts_airport a on cast(c.airport_id as int) = a.airport_id
where
    rnk <= {n}
order by
    rnk asc
"""
cancel_by_airport_df = execute_query(cancel_by_airport_query, trino_creds)

airline_tab, airport_tab = st.tabs(["By Airport", "By Airline"])
with airport_tab:
    st.markdown(
        f"## Top Airports with Highest Number of Cancelled Flights In the Last 7 Days",
        unsafe_allow_html=False,
    )
    show_table(cancel_by_airport_df)
with airline_tab:
    st.markdown(
        f"## Top Airlines with Highest Number of Cancelled Flights In the Last 7 Days",
        unsafe_allow_html=False,
    )
    show_table(cancel_by_airline_df)

airport_market_share_query = f"""
WITH
  interval_data AS (
    SELECT
      *
    FROM
      sarneski44638.airport_airline_daily
    WHERE
      flightdate BETWEEN date('{fake_today}') - interval '6' DAY AND date('{fake_today}')
  )
SELECT
  d.airport_id,
  a.airport_abbrev AS airport_abbreviation,
  a.airport_location,
  sum(n_seats) AS total_seats,
  100.0 * sum(n_seats) / (
    SELECT
      sum(n_seats)
    FROM
      interval_data
  ) AS market_share
FROM
  interval_data d
  JOIN sarneski44638.dim_bts_airport a ON cast(d.airport_id AS int) = a.airport_id
GROUP BY
  d.airport_id,
  a.airport_abbrev,
  a.airport_location
ORDER BY
  market_share DESC
"""
airport_market_share_df = execute_query(airport_market_share_query, trino_creds)
hover_data = [
    "airport_location",
    "total_seats",
]
hovertemplate = (
    "Airport: %{x}<br>"
    + "Location: %{customdata[0]}<br>"
    + "%{y}% market share<br>"
    + "%{customdata[1]:,} number of seats<br>"
    + "<extra></extra>"
)
fig = create_market_share_plot(
    airport_market_share_df,
    x="airport_abbreviation",
    y="market_share",
    title="Market Share By Airport over Last 7 Days",
    x_title="Airport",
    y_title="Market Share",
    hover_data=hover_data,
    hover_template=hovertemplate,
)
st.plotly_chart(fig, use_container_width=True)
