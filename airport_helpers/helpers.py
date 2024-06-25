import fnmatch
from io import StringIO
import os
import pandas as pd
import pathlib
import time
from zipfile import Path


def get_data_path():
    return pathlib.Path(os.environ["AIRPORT_DATA"])


def get_output_directory(data_source, folder_name, region):
    dest_path = get_data_path() / data_source / folder_name / region
    dest_path.mkdir(parents=True, exist_ok=True)
    return dest_path


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(
            f"Function {func.__name__} took {end_time - start_time:.2f} seconds to complete."
        )
        return result

    return wrapper


@timing_decorator
def get_zipfiles(data_source, region):
    fpath = get_data_path() / data_source / "downloads" / region
    return sorted(fpath.glob("*.zip"))


@timing_decorator
def read_file(zipfile):
    zipped = Path(zipfile)
    csv_file = fnmatch.filter(zipped.root.namelist(), "*.csv")
    data = zipped.joinpath(*csv_file).read_text()
    df = pd.read_csv(StringIO(data), low_memory=False)
    df.columns = [x.lower() for x in df.columns]
    return df


@timing_decorator
def extract_data(process_data, data_source, region):
    output_dir = get_output_directory(data_source, "transformed", region)
    zip_files = get_zipfiles(data_source, region)
    for zip_file in zip_files:
        basename = zip_file.name
        df = read_file(zip_file)
        df = process_data(df)
        output_fname = output_dir / basename.replace(".zip", ".parquet")
        df.to_parquet(output_dir / output_fname)
