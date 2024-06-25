# %%
import pathlib
import zipfile

import airport_helpers as air
from utils.helpers import get_secret, upload_to_s3


def extract_raw_files(data_source, region, suffix=".csv"):
    zip_files = air.get_zipfiles(data_source, region)
    output_dir = air.get_data_path() / data_source / "raw_files"
    output_dir.mkdir(parents=True, exist_ok=True)
    files_to_upload = set()
    print(f"Will process {len(zip_files):,} zipped files")
    for zip_fpath in zip_files:
        with zipfile.ZipFile(zip_fpath, "r") as zip_ref:
            unzipped_fpaths = [pathlib.Path(output_dir) / f for f in zip_ref.namelist()]
            files_to_upload.update(unzipped_fpaths)
            zip_ref.extractall(output_dir)
            print(
                f"Extracted {len(unzipped_fpaths)=:,} files {[f.name for f in unzipped_fpaths]} from {zip_fpath}"
            )
    if suffix:
        files_to_upload = [f for f in files_to_upload if f.suffix == suffix]
    suffix_str = f"{suffix} " if suffix else ""
    print(f"{len(files_to_upload)=:,} {suffix_str}files to upload")
    return files_to_upload


if __name__ == "__main__":
    region = "all"
    data_source = "bts_flights"
    files_to_upload = extract_raw_files(data_source, region)
    namespace = "sarneski44638"
    s3_bucket = get_secret("AWS_S3_BUCKET_TABULAR")
    for local_file in files_to_upload:
        s3_file = f"{namespace}/raw_{data_source}/{local_file.name}"
        upload_to_s3(local_file, s3_bucket, s3_file)

# %%
