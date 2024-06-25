# %%
from calendar import month_name
import os

from airport_helpers import web_scraping as ws


def run_bts_t100_process(
    driver, region, year, period, pre_zipped, international=False
):
    turn_on = [
        "PASSENGERS",
        "SEATS",
        "FREIGHT",
        "PAYLOAD",
        "MAIL",
        "DISTANCE",
        "RAMP_TO_RAMP",
        "AIR_TIME",
        "DEPARTURES_SCHEDULED",
        "DEPARTURES_PERFORMED",
        "AIRLINE_ID",
        "CARRIER",
        "ORIGIN",
        "DEST",
        "AIRCRAFT_TYPE",
        "YEAR",
        "QUARTER",
        "MONTH",
    ]
    if international:
        turn_on += ["ORIGIN_COUNTRY", "DEST_COUNTRY"]
    ws.deselect_all_checkboxes(driver)
    ws.select_listed_boxes(driver, turn_on)
    ws.select_and_download(driver, region, year, period, pre_zipped)


# %%
if __name__ == "__main__":
    for state in ["all"]:
        for year in [2023, 2022, 2021, 2020]:
            for month in range(1, 12 + 1):
                ws.download_and_save_data(
                    data_source="bts_t100_domestic",
                    url=os.environ["BTS_T100_DOMESTIC_FLIGHTS_URL"],
                    region=state,
                    year=year,
                    period=month_name[month],
                    pre_zipped=False,
                    run_process=run_bts_t100_process,
                )
                ws.download_and_save_data(
                    data_source="bts_t100_international",
                    url=os.environ["BTS_T100_INTERNATIONAL_FLIGHTS_URL"],
                    region=state,
                    year=year,
                    period=month_name[month],
                    pre_zipped=False,
                    run_process=run_bts_t100_process,
                    international=True,
                )


# %%
