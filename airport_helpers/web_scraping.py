import glob
import os
import pathlib
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import shutil
import time
import uuid

from .helpers import get_data_path


def wait_for_download(path, n_tries=600, filename="*"):
    count = 0
    while True:
        filenames = glob.glob(str(pathlib.Path(path) / filename))
        if len(filenames) > 0:
            print(f"found {filenames=}")
            return filenames[0]
        print(f"waiting for download {count=}")
        time.sleep(5)
        count += 1
        if count > n_tries:
            print(f"exceeded {n_tries=} failed to find {filename=}")
            return None


def get_driver(download_path):
    service = Service(executable_path="/opt/homebrew/bin/chromedriver")
    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": str(download_path),
    }
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def deselect_all_checkboxes(driver):
    """Deselect all checkboxes"""
    checkboxes = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
    for box in checkboxes:
        if box.is_selected():
            box.click()


def select_listed_boxes(driver, select_list):
    """Select all boxes in list"""
    for name in select_list:
        box = driver.find_element(By.NAME, name)
        if box.is_selected() == False:
            box.click()


def select_and_download(driver, region, year, period, pre_zipped=True):
    if pre_zipped:
        box = driver.find_element(By.NAME, "chkDownloadZip")
        if box.is_selected() == False:
            box.click()
    # Filter by geography
    elem = driver.find_element(By.NAME, "cboGeography")
    elem.send_keys(region)
    # Filter by year
    elem = driver.find_element(By.NAME, "cboYear")
    elem.send_keys(str(year))
    # Filter by period
    elem = driver.find_element(By.NAME, "cboPeriod")
    elem.send_keys(period)
    driver.find_element(By.NAME, "btnDownload").click()


def get_download_fname_pattern(data_type):
    fname_pattern_dict = {
        "bts_flights": "On_Time_Reporting*.zip",
        "bts_t100_international": "DL_SelectFields.zip",
        "bts_t100_domestic": "DL_SelectFields.zip",
        "bts_db1b": "Origin_and_Destination*.zip",
    }
    if not data_type in fname_pattern_dict:
        raise Exception(f"Unexpected data_type {data_type=}")
    return fname_pattern_dict[data_type]


def create_tmp_download_folder():
    download_path = os.path.join(os.getcwd(), f"download-{uuid.uuid4()}")
    if os.path.exists(download_path):
        shutil.rmtree(download_path)
    download_path = pathlib.Path(download_path)
    download_path.mkdir(parents=True, exist_ok=True)
    return download_path


def get_destination_filename(data_source, region, year, period):
    dest_path = get_data_path() / data_source / "downloads" / region.lower()
    dest_path.mkdir(parents=True, exist_ok=True)
    new_filename = f"bts-{year}-{period.lower().replace(' ', '-')}.zip"
    return dest_path / new_filename


def download_and_save_data(
    data_source, url, region, year, period, pre_zipped, run_process, **kwargs
):
    download_fname_pattern = get_download_fname_pattern(data_source)
    tmp_download_folder = create_tmp_download_folder()
    destination_filename = get_destination_filename(data_source, region, year, period)

    if not os.path.exists(destination_filename):
        driver = get_driver(tmp_download_folder)
        driver.get(url)
        if pre_zipped:
            select_and_download(driver, region, year, period)
        else:
            run_process(driver, region, year, period, pre_zipped, **kwargs)
        download_filename = wait_for_download(
            tmp_download_folder, filename=download_fname_pattern
        )
        if download_filename is not None:
            shutil.move(download_filename, destination_filename)
        driver.close()
        driver.quit()
        time.sleep(10)
    if os.path.exists(tmp_download_folder):
        shutil.rmtree(tmp_download_folder)
