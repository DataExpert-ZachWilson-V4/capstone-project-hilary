# %%
from calendar import month_name
import os

from airport_helpers import web_scraping as ws


if __name__ == "__main__":
    data_source = "bts_flights"
    url=os.environ["BTS_FLIGHTS_URL"]
    for state in ["all"]:
        for year in [2023, 2022, 2021, 2020]:
            for month in range(1, 12 + 1):
                ws.download_and_save_data(
                    data_source=data_source,
                    url=url,
                    region=state,
                    year=year,
                    period=month_name[month],
                    pre_zipped=True,
                    run_process=None
                )

# %%
