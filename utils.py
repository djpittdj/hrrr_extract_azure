import os
from dateutil import tz
from datetime import datetime
from pathlib import Path

regex_local = r".*hrrr.(\d{8}).t(\d{2})z.wrfsfcf(\d{2}).grib2"
regex_remote = r"gs:.*/hrrr.(\d{8})/conus/hrrr.t(\d{2})z.wrfsfcf(\d{2}).grib2"

box = (33, 38, -100.5, -93)
unit_mps_mph = 2.23694
dttm_format = "%Y-%m-%d %H:%M:%S"

storm_dir = Path("")

# definition of UTC and local timezones
from_zone = tz.gettz("UTC")
to_zone = tz.gettz("America/Chicago")

"""
HRRR variable index in different versions
                                        V2      V3      V4
temperature 2m:                         54      66      71
wind 10m:                               61      73      79
wind 10m u component:                   59      71      77
wind 10m v component:                   60      72      78
total precipitation(0-hour):            64      78      84
total precipitation(hour_prev-hour):            84      90
total snowfall:                         48      60      65
freezing rain:                           0      81      87
water eq. accm. snow:                   65      79      85
composite reflectivity:                          1       1
wind gust:                                       8       9
CAPE 255 (hour 00, 01) :                       138     154
CAPE 255 (hour 02+):                           141     157
hourly max updraft helicity 2km-5km:            43      45
lightning potential index 1m ground                     57
"""

dict_var_index = {"temperature_2m": {"v2":54, "v3":66, "v4":71},
                  "wind_10m": {"v2":61, "v3":73, "v4":79},
                  "wind_10m_u": {"v2":59, "v3":71, "v4":77},
                  "wind_10m_v": {"v2":60, "v3":72, "v4":78},
                  "precipitation_tot": {"v2":64, "v3":78, "v4":84},
                  "precipitation_tot_past_h": {"v2":0, "v3":84, "v4":90},
                  "snowfall_tot": {"v2":48, "v3":60, "v4":65},
                  "freezing_rain": {"v2":0, "v3":81, "v4":87},
                  "wat_eq_accm_snow": {"v2":65, "v3":79, "v4":85},
                  "composite_reflectivity": {"v2":0, "v3":1, "v4":1},
                  "wind_gust": {"v2":0, "v3":8, "v4":9},
                  "CAPE255_h00_h01": {"v2":0, "v3":138, "v4":154},
                  "CAPE255_h02plus": {"v2":0, "v3":141, "v4":157},
                  "helicity": {"v2":0, "v3":43, "v4":45},
                  "lightning": {"v2":0, "v3": 0, "v4":57}
                 }

def get_hrrr_ver(hour_forecast, n_messages):
    """get the version of HRRR GRIB2 file based on its forecast hour and number of variables"""
    v = "v0"
    if hour_forecast in ["00", "01"]:
        if n_messages == 148:
            v = "v3"
        elif n_messages == 170:
            v = "v4"
    else:
        if n_messages == 151:
            v = "v3"
        elif n_messages == 173:
            v = "v4"
    return v

def filename_to_timestamp(filename):
    basename = filename.stem
    date = basename.split('.')[1]
    hour = basename.split('.')[2][1:3]
    timestamp = datetime.strptime(f"{date} {hour}:00:00", dttm_format)

    return timestamp

def timestamp_to_filename(timestamp, hour_forecast, path):
    date = timestamp.strftime("%Y%m%d")
    hour_analysis = timestamp.strftime("%H")

    filename = Path(f"{str(path)}/hrrr.{date}.t{hour_analysis}z.wrfsfcf{hour_forecast}.grib2")

    return filename

def cross_section_df(df_input, box):
    """take a df with at least lat, lon and a variable, return another df depending on the box"""
    lat_south, lat_north, lon_west, lon_east = box[0], box[1], box[2], box[3]
    df_output = df_input.loc[(df_input.lat>=lat_south) &
                             (df_input.lat<=lat_north) &
                             (df_input.lon>=lon_west) &
                             (df_input.lon<=lon_east)]

    return df_output

def angle360(x):
    if x<0:
        return x+360
    else:
        return x

def angle_desc(x):
    if x>=337.5 or x<22.5:
        return "W"
    elif x>=22.5 and x<67.5:
        return "SW"
    elif x>=67.5 and x<112.5:
        return "S"
    elif x>=112.5 and x<157.5:
        return "SE"
    elif x>=157.5 and x<202.5:
        return "E"
    elif x>=202.5 and x<247.5:
        return "NE"
    elif x>=247.5 and x<292.5:
        return "N"
    elif x>=292.5 and x<337.5:
        return "NW"

def Kelvin_to_Fahrenheit(x):
    return (x - 273.15) * 9/5 + 32

def str_local_timestamp(x):
    """UTC timestamp to local timestamp string"""
    ret = datetime.strptime(x, "%Y-%m-%d %H:%M:%S").replace(tzinfo=from_zone).astimezone(to_zone).strftime(dttm_format)
    return ret

def filter_hours(x):
    """only return these hours"""
    if "t00z" in x or "t06z" in x or "t12z" in x or "t18z" in x:
        return True
    else:
        return False

def get_lst_diff(lst1, lst2):
    """return a sorted list of the difference between two lists"""
    lst_diff = list(set(lst1).difference(set(lst2)))
    lst_diff.sort()
    return lst_diff
