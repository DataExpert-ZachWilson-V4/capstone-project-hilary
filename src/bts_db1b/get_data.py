# %%
import os

from airport_helpers import web_scraping as ws


if __name__ == "__main__":
    data_source = "bts_db1b"
    url = os.environ["BTS_DB1B_URL"]
    for state in ["all"]:
        for year in [2023, 2022, 2021, 2020]:
            for quarter in range(1, 4 + 1):
                ws.download_and_save_data(
                    data_source=data_source,
                    url=url,
                    region=state,
                    year=year,
                    period=f"Quarter {quarter}",
                    pre_zipped=True,
                    run_process=None
                )

# %%
