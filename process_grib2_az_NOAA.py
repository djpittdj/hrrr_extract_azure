from process_grib2_az import *

url_NOAAHRRR = "https://noaahrrr.blob.core.windows.net/hrrr/hrrr.INPUT_DATE/conus/hrrr.tINPUT_HOURz.wrfsfcf00.grib2"

if __name__ == "__main__":
    workspace = Workspace(subscription_id, resource_group, workspace_name)

    # blob service client, i.e., container
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container=container_name)

    lst_unique_grid = Dataset.get_by_name(workspace, name="unique_grid_id").to_pandas_dataframe()["hrrr_id"].tolist()

    dttms = pd.date_range("2021-11-07", "2021-12-01", freq="h", closed="left")
    for input_dttm in dttms:
        input_d = input_dttm.strftime("%Y%m%d")
        input_h = input_dttm.strftime("%H")
        url_grib2 = url_NOAAHRRR.replace("INPUT_DATE", input_d).replace("INPUT_HOUR", input_h)
        process_grib2_az(url_grib2, lst_unique_grid, container_client)
