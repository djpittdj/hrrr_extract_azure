"""use the cross section function to generate a list of index
that represents the territory of OGE
output the unique grid id that has premise associated"""
import pygrib
from utils import dict_var_index, cross_section_df, box, work_dir, get_hrrr_ver, regex_local
import pandas as pd
import pickle
import re
from pathlib import Path

if __name__ == "__main__":
    filename = Path(work_dir/"hrrr.20200101.t00z.wrfsfcf00.grib2")
    matched = re.findall(regex_local, str(filename))[0]
    date_str, analysis_hour_str, forecast_hour_str = matched
    grbs = pygrib.open(f"{str(filename)}")
    version_hrrr = get_hrrr_ver(forecast_hour_str, grbs.messages)

    index_temperature_2m = dict_var_index["temperature_2m"][version_hrrr]
    grb_temperature_2m = grbs[index_temperature_2m]

    # lat and lon matrices
    lat, lon = grb_temperature_2m.latlons()[0], grb_temperature_2m.latlons()[1]

    df = pd.DataFrame({"lat": lat.flatten(), "lon": lon.flatten()})
    df2 = cross_section_df(df, box)
    # hrrr_id is the id in the HRRR model, there are about 2 million points in the HRRR model
    df2 = df2.reset_index()
    df2 = df2.rename(columns={"index":"hrrr_id"})
    # grid_id is the id in the OGE territory
    df2 = df2.reset_index()
    df2 = df2.rename(columns={"index":"grid_id"})

    # unique grid id that has premise associated: Premise_Peters_small has the premise associated with grid_id
    df_premise_peters = pd.read_csv(f"{str(work_dir)}/data_GIS/Premise_Peters_small.csv", usecols=["PETER_ID"])
    df_grid_unique = df_premise_peters.rename(columns={"PETER_ID":"grid_id"})["grid_id"].drop_duplicates()

    df_grid_unique2 = pd.merge(df2, df_grid_unique, on="grid_id")
    df_grid_unique2.to_csv(f"{work_dir}/data_GIS/unique_grid_id.csv", index=False)