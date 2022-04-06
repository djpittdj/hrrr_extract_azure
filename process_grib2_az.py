from azure.storage.blob import BlobServiceClient
from azureml.core import Workspace, Dataset
from grib2data import grib2data
from multiprocessing import Pool
from functools import partial
import pandas as pd
import re
from datetime import datetime

def name_to_timestamp(x):
    """convert url to timestamp, it works for both grib2 and csv urls"""
    regex = r".*hrrr.(\d{8}).*hrrr.t(\d{2})z.*"
    d, t = re.findall(regex, x)[0]
    
    return datetime.strptime(f"{d} {t}:00:00", "%Y%m%d %H:%M:%S")


# environment variables
subscription_id = ''
resource_group = ''
workspace_name = ''
connect_str = ""
container_name = ""


def process_grib2_az(url_grib2, lst_unique_grid, container_client):
    grib2 = grib2data(url_grib2, lst_unique_grid)
    try:
        grib2.get_vars()
    except (TypeError, SystemExit):
        pass
    grib2.write_to_disk()
    grib2.upload_blob(container_client)
    grib2.remove_from_disk()


if __name__ == "__main__":
    workspace = Workspace(subscription_id, resource_group, workspace_name)

    # blob service client, i.e., container
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container=container_name)

    # this list is the full blob name of timestamps where the file 
    # for prev analysis hour + 1 forecast hour is also present
    blob_list_grib2_f00_full = Dataset.\
                                get_by_name(workspace, name="blob_list_grib2_f00_full").\
                                to_pandas_dataframe()["Column1"].\
                                tolist()

    # grid points with assets
    lst_unique_grid = Dataset.get_by_name(workspace, name="unique_grid_id").to_pandas_dataframe()["hrrr_id"].tolist()
    
    # find out which CSV files are missing, i.e., a list of files to process
    # list csv files
    blob_iter = container_client.list_blobs(name_starts_with="CSV/hrrr")
    blob_list = []
    for blob in blob_iter:
        blob_list.append(blob["name"])
    blob_list_csv = list(filter(lambda x: x.endswith(".csv"), blob_list))

    # availabe grib2 in df
    df_grib2 = pd.DataFrame({"dttm_grib2":list(map(name_to_timestamp, blob_list_grib2_f00_full))})

    # availabe csv in a df
    df_csv = pd.DataFrame({"dttm_csv":list(map(name_to_timestamp, blob_list_csv))})

    df_merge = pd.merge(df_grib2, df_csv, left_on="dttm_grib2", right_on="dttm_csv", how="left")
    indices_2b_processed = df_merge.loc[df_merge.dttm_csv.isnull()].index.tolist()
    url_2b_processed = [blob_list_grib2_f00_full[i] for i in indices_2b_processed]

    # serial execution
    for (i, url_grib2) in enumerate(url_2b_processed):
        process_grib2_az(url_grib2, lst_unique_grid, container_client)
        print(i, url_grib2, flush=True)

    # using thread pool
    # with Pool(processes=2) as pool:
    #     pool.map(partial(process_grib2_az, lst_unique_grid=lst_unique_grid), 
    #             url_2b_processed)
