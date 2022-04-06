from utils import dttm_format, get_hrrr_ver
import tempfile
import requests
import xarray as xr
from datetime import datetime, timedelta
from numpy import array, where
from pathlib import Path
from collections import OrderedDict
import re
from utils import dict_var_index
import pandas as pd
import uuid

# regex string to extract analysis date, analysis hour and valid hour from url_grib2
rs_url_grib2 = r".*hrrr.(\d{8})/conus/hrrr.t(\d{2})z.wrfsfcf(\d{2}).grib2"


# create a dict with mapping between var_num and its byte range
# the last variable has range as range_start-;
# the other variables have range as range_start-range_end;
# this dict is going to be used for all variables
def get_var_br(idx):
    """Get byte range for variables
    input: idx / list
    output: OrderedDict of variables to their byte ranges"""
    d = OrderedDict()
    for (i,j) in zip(idx, idx[1:]+[None]):
        var_num, range_start, _, var_name, var_desc, _, _ = i.split(':')
        var_num, var_desc = int(var_num), f"{var_name}: {var_desc}"
        if var_num < len(idx):
            range_end = j.split(':')[1]
        else:
            range_end = ''
        d[int(var_num)] = (f"{range_start}-{range_end}", var_desc)
    
    return d


def get_url_grib2_relative(url_grib2):
    """get relative path of url_grib2 in the container"""
    relative_blob_url = ''
    if "noaahrrr.blob.core.windows.net" not in url_grib2:
        rs = r"https.*hrrrdata/(.*)"
        relative_blob_url = re.findall(rs, url_grib2)[0]
    else:
        rs = r"https://noaahrrr.blob.core.windows.net/hrrr/(.*)"
        relative_blob_url = re.findall(rs, url_grib2)[0]
        relative_blob_url = f"GRIB2/{relative_blob_url}"
    
    return relative_blob_url


def get_url_root(url_grib2):
    """split the url_grib2 into two parts: 
     (root) and
    hrrr.20190101/conus/hrrr.t00z.wrfsfcf00.grib2""" 
    rs_url_root = r"(.*)/hrrr.\d{8}/conus/hrrr.t\d{2}z.wrfsfcf\d{2}.grib2"
    url_root = re.findall(rs_url_root, url_grib2)[0]
    
    return url_root


def timestamp_to_url(timestamp, hour_forecast, url_root):
    """contruct a url for a blob based on timestamps and base url"""
    date = timestamp.strftime("%Y%m%d")
    hour_analysis = timestamp.strftime("%H")
    
    url_output = f"{url_root}/hrrr.{date}/conus/hrrr.t{hour_analysis}z.wrfsfcf{hour_forecast}.grib2"
    
    return url_output


class grib2data(object):
    """based on url_grib2 and grids, construct an object that contains relevant data of this grib2 file"""
    def __init__(self, url_grib2, lst_unique_grid):
        self.url_grib2 = url_grib2
        self.lst_unique_grid = lst_unique_grid
        self.df = pd.DataFrame()
        # to name output dataframe
        self.df_fname = f"/tmp/df_{uuid.uuid4()}.csv"

        self.url_idx = f"{self.url_grib2}.idx"
        self.analysis_date_str, self.analysis_hour_str, self.valid_hour_str = \
            re.findall(rs_url_grib2, self.url_grib2)[0]
        self.analysis_dttm = datetime.strptime(f"{self.analysis_date_str} {self.analysis_hour_str}:00:00", "%Y%m%d %H:%M:%S")
        self.analysis_dttm_str = self.analysis_dttm.strftime(dttm_format)
        self.analysis_dttm_prev_h = self.analysis_dttm - timedelta(hours=1)
        self.url_grib2_b = None

        self.valid_hour = int(self.valid_hour_str)
        self.valid_dttm = self.analysis_dttm + timedelta(hours=self.valid_hour)
        self.valid_dttm_str = self.valid_dttm.strftime(dttm_format)
        
        # get index for url_grib2
        try:
            r = requests.get(self.url_idx)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

        self.idx = None
        if r.status_code == 200:
            self.idx = r.text.splitlines()
        elif r.status_code == 404:
            raise TypeError

        self.n_vars = len(self.idx)
        self.version_hrrr = get_hrrr_ver(self.valid_hour_str, self.n_vars)

        self.dict_var_br = get_var_br(self.idx)

        self.url_root = get_url_root(self.url_grib2)

    def __str__(self):
        return f"{self.url_grib2} : {self.version_hrrr}"

    def get_one_var(self, url_grib2, var_num, dict_var_br):
        """get the values of one variable.
        url_grib2, var_num and dict_var_br are all subject to change,
        but self.lst_unique_grid is static.
        input: var_num / int: variable index in HRRR, starting with 1
        output: a list of values for this variable for only grid_id with OGE assets"""
        if var_num != 0:
            # create tmp file for this var_num and generate its values
            file_tmp = tempfile.NamedTemporaryFile(prefix=f"tmp_{var_num}_", delete=False)
            headers = {"Range":f"bytes={dict_var_br[var_num][0]}"}
            try:
                resp = requests.get(url_grib2, headers=headers, stream=True)
            except requests.exceptions.RequestException as e:
                raise SystemExit(e)

            file_tmp.write(resp.content)
            file_tmp.close()
            ds = xr.open_dataset(file_tmp.name, engine='cfgrib', backend_kwargs={'indexpath':''})
            df_tmp = ds.to_dataframe().reset_index()
            Path(file_tmp.name).unlink()
            values = df_tmp.iloc[:,-1].values[self.lst_unique_grid]
        else:
            # this variable is not present in the grib2 file
            values = array([0]*len(self.lst_unique_grid))

        return values

    def get_vars(self):
        """get values for all variables"""
        index_temperature_2m = dict_var_index["temperature_2m"][self.version_hrrr]
        index_wind_10m_u = dict_var_index["wind_10m_u"][self.version_hrrr]
        index_wind_10m_v = dict_var_index["wind_10m_v"][self.version_hrrr]
        index_wind_10m = dict_var_index["wind_10m"][self.version_hrrr]
        index_composite_reflectivity = dict_var_index["composite_reflectivity"][self.version_hrrr]
        index_wind_gust = dict_var_index["wind_gust"][self.version_hrrr]
        index_helicity = dict_var_index["helicity"][self.version_hrrr]
        index_lightning = dict_var_index["lightning"][self.version_hrrr]

        values_temperature_2m = self.get_one_var(self.url_grib2, index_temperature_2m, self.dict_var_br)
        values_wind_10m_u = self.get_one_var(self.url_grib2, index_wind_10m_u, self.dict_var_br)
        values_wind_10m_v = self.get_one_var(self.url_grib2, index_wind_10m_v, self.dict_var_br)
        values_wind_10m = self.get_one_var(self.url_grib2, index_wind_10m, self.dict_var_br)
        values_composite_reflectivity = self.get_one_var(self.url_grib2, index_composite_reflectivity, self.dict_var_br)
        values_wind_gust = self.get_one_var(self.url_grib2, index_wind_gust, self.dict_var_br)
        values_helicity = self.get_one_var(self.url_grib2, index_helicity, self.dict_var_br)
        values_lightning = self.get_one_var(self.url_grib2, index_lightning, self.dict_var_br)

        # initialize timestamp dependent index
        index_precipitation = 0 # for forecast hours 0 and 1
        index_precipitation_past_h = 0 # for forecast hours other than 0 and 1
        index_snowfall = 0
        index_freezerain = 0
        index_CAPE255 = 0

        if self.valid_hour == 0:
            # use precipitation, snowfall, freezing rain and helicity
            # from previous analysis hour and forecast 1 hour as data 
            # for valid hour 0
            # use _b to indicate the previous analysis hour + forecast 1 hour
            url_grib2_b = timestamp_to_url(self.analysis_dttm_prev_h, "01", get_url_root(self.url_grib2))
            url_idx_b = f"{url_grib2_b}.idx"
            # the blob name supplied to the get_blob_client is relevent, i.e., GRIB2/hrrr.
            # local_blob_url = get_url_grib2_relative(self.url_grib2)

            try:
                r = requests.get(url_idx_b)
            except requests.exceptions.RequestException as e:
                raise SystemExit(e)

            idx_b = None
            if r.status_code == 200:
                idx_b = r.text.splitlines()
            elif r.status_code == 404:
                raise TypeError

            dict_var_br_b = get_var_br(idx_b)
            version_hrrr_b = get_hrrr_ver("01", len(idx_b))

            index_precipitation_b = dict_var_index["precipitation_tot"][version_hrrr_b]
            index_snowfall_b = dict_var_index["snowfall_tot"][version_hrrr_b]
            index_freezerain_b = dict_var_index["freezing_rain"][version_hrrr_b]

            # values
            values_precipitation = self.get_one_var(url_grib2_b, index_precipitation_b, dict_var_br_b)
            values_snowfall = self.get_one_var(url_grib2_b, index_snowfall_b, dict_var_br_b)
            values_freezerain = self.get_one_var(url_grib2_b, index_freezerain_b, dict_var_br_b)

            index_CAPE255 = dict_var_index["CAPE255_h00_h01"][self.version_hrrr]
            values_cape = self.get_one_var(self.url_grib2, index_CAPE255, self.dict_var_br)
        elif self.valid_hour == 1:
            # valid hour 1 has data for precipitation snowfall and freezing rain for hour 0-1
            index_precipitation = dict_var_index["precipitation_tot"][self.version_hrrr]
            index_snowfall = dict_var_index["snowfall_tot"][self.version_hrrr]
            index_freezerain = dict_var_index["freezing_rain"][self.version_hrrr]

            index_CAPE255 = dict_var_index["CAPE255_h00_h01"][self.version_hrrr]

            values_precipitation = self.get_one_var(self.url_grib2, index_precipitation, self.dict_var_br)
            values_snowfall = self.get_one_var(self.url_grib2, index_snowfall, self.dict_var_br)
            values_freezerain = self.get_one_var(self.url_grib2, index_freezerain, self.dict_var_br)

            values_cape = self.get_one_var(self.url_grib2, index_CAPE255, self.dict_var_br)
        elif self.valid_hour >= 2:
            # for hour 2 and forward, precipitation has the data for (valid_hour-1) to valid_hour.
            # but for snowfall and freezing rain, it has data for hour 0 to valid_hour, so the past hour data is
            # calculated as the difference: (0 to valid_hour) - (0 to (valid_hour-1))
            valid_prev_h = int(self.valid_hour_str) - 1
            valid_prev_h_str = f"{valid_prev_h:02}"

            index_precipitation_past_h = dict_var_index["precipitation_tot_past_h"][self.version_hrrr]
            index_snowfall = dict_var_index["snowfall_tot"][self.version_hrrr]
            index_freezerain = dict_var_index["freezing_rain"][self.version_hrrr]

            # CAPE has different index for forecast hour 00, 01 and hours 02+
            index_CAPE255 = dict_var_index["CAPE255_h02plus"][self.version_hrrr]

            # same analysis date and analysis hour, but previous forecast hour
            url_grib2_prev_h = self.url_grib2.replace(f"wrfsfcf{self.valid_hour_str}", f"wrfsfcf{valid_prev_h_str}")
            url_idx_prev_h = f"{url_grib2_prev_h}.idx"

            try:
                r = requests.get(url_idx_prev_h)
            except requests.exceptions.RequestException as e:
                raise SystemExit(e)
            
            idx_prev_h = None
            if r.status_code == 200:
                idx_prev_h = r.text.splitlines()
            elif r.status_code == 404:
                raise TypeError

            dict_var_br_prev_h = get_var_br(idx_prev_h)

            values_snowfall = self.get_one_var(self.url_grib2, index_snowfall, self.dict_var_br) - \
                              self.get_one_var(url_grib2_prev_h, index_snowfall, dict_var_br_prev_h)
            values_snowfall = where(values_snowfall<0, 0.0, values_snowfall)
            values_freezerain = self.get_one_var(self.url_grib2, index_freezerain, self.dict_var_br) - \
                                self.get_one_var(url_grib2_prev_h, index_freezerain, dict_var_br_prev_h)
            values_freezerain = where(values_freezerain<0, 0.0, values_freezerain)

            values_precipitation = self.get_one_var(self.url_grib2, index_precipitation_past_h, self.dict_var_br)
            values_cape = self.get_one_var(self.url_grib2, index_CAPE255, self.dict_var_br)

        # assemble into one df
        self.df = pd.DataFrame({"timestamp_analysis": self.analysis_dttm_str,
                                "timestamp_valid": self.valid_dttm_str,
                                "temperature": values_temperature_2m,
                                "wind_10m_u": values_wind_10m_u,
                                "wind_10m_v": values_wind_10m_v,
                                "wind_10m": values_wind_10m,
                                "precipitation_tot": values_precipitation,
                                "snowfall_tot": values_snowfall,
                                "freezing_rain": values_freezerain,
                                "composite_reflectivity": values_composite_reflectivity,
                                "wind_gust": values_wind_gust,
                                "CAPE255": values_cape,
                                "helicity": values_helicity,
                                "lightning": values_lightning,
                                "hrrr_id": self.lst_unique_grid
                                })
    
    def write_to_disk(self):
        """save the df to disk"""
        self.df.to_csv(self.df_fname, index=False)
    
    def get_url_csv_relative(self):
        """get relative url_csv from url_grib2, i.e,:
        GRIB2/hrrr.20190101/conus/hrrr.t00z.wrfsfcf00.grib2 ->
        CSV/hrrr.20190101/conus/hrrr.t00z.f00.csv
        :input url_grib2 string 
        """
        url_grib2_relative = get_url_grib2_relative(self.url_grib2)

        url_csv_relative = (url_grib2_relative
                           .replace("GRIB2", "CSV")
                           .replace(".grib2", ".csv")
                           .replace("wrfsfc", ''))

        return url_csv_relative
    
    def upload_blob(self, container_client):
        blob_client_csv = container_client.get_blob_client(blob=self.get_url_csv_relative())
        with open(self.df_fname, "rb") as data:
            blob_client_csv.upload_blob(data, overwrite=True)

    def remove_from_disk(self):
        """remove df"""
        Path(self.df_fname).unlink()
