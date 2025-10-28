import datetime as dt
import glob
import os

import cfgrib
import requests
import xarray as xr
from dask.distributed import wait, Client as DaskClient

def request_url_process_data(url, grib_file_path, nc_file_path, date_str, run, step):
    response = requests.get(url)

    if response.status_code == 200:
        with open(grib_file_path, "wb") as f:
            f.write(response.content)
    else:
        print(f"Couldn't download {date_str} {run}z {step}h - Status code: {response.status_code}")
        return False

    ds_list = cfgrib.open_datasets(grib_file_path)
    ds_list_final = []
    vars = ['t2m', 'd2m', 'msl', 'u10', 'v10', 'tcc']
    for ds_n in ds_list:
        var_names = [var for var in vars if var in ds_n.data_vars]
        if len(var_names) > 0:
            if 'heightAboveGround' in ds_n.coords:
                ds_n = ds_n.drop_vars('heightAboveGround')
            ds_list_final.append(ds_n[var_names])

    ds = xr.merge(ds_list_final, compat="no_conflicts")
    ds = ds.sel(latitude=slice(52, 51.25), longitude=slice(-1.5, -0.75))
    ds.to_netcdf(nc_file_path)
    print(f"Saved {nc_file_path}")

    ds.close()
    for f in glob.glob(f"{grib_file_path}*"):
        os.remove(f)

def url_picker(url_base: str, model: str, date: dt.datetime, run: str, step: int) -> str|None:
    date_str = date.strftime("%Y%m%d")

    if model == "aifs":
        model = "aifs-single"

    url_dict = {
        "url1":f"{url_base}/{date_str}/{run}z/0p4-beta/oper/{date_str}{run}0000-{step}h-oper-fc.grib2",
        "url2":f"{url_base}/{date_str}/{run}z/0p25/oper/{date_str}{run}0000-{step}h-oper-fc.grib2",
        "url3":f"{url_base}/{date_str}/{run}z/{model}/0p25/oper/{date_str}{run}0000-{step}h-oper-fc.grib2",
        }
    
    if dt.datetime(2023, 1, 18) <= date <= dt.datetime(2024, 1, 31):
        if date == dt.datetime(2024, 1, 31) and (run in ["06", "12", "18"]):
            return url_dict["url2"]
        else:
            return url_dict["url1"]
    elif dt.datetime(2024, 2, 1) <= date <= dt.datetime(2024, 2, 28):
        if date == dt.datetime(2024, 2, 28) and (run in ["06", "12", "18"]):
            return url_dict["url3"]
        else:
            return url_dict["url2"]
    elif dt.datetime(2024, 3, 1) <= date:
        return url_dict["url3"]

if __name__=="__main__":
    dask_client = DaskClient(n_workers=3, threads_per_worker=2)

    url_base = "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com"
    model = "ifs"

    # Create a list of dates to be downloaded
    start_dt = dt.datetime(2024, 10, 24)
    end_dt = dt.datetime(2025, 10, 24)

    if end_dt > dt.datetime.now():
        print("End date is in the future, setting to today's date")
        end_dt = dt.datetime.now()
    if model == "aifs" and start_dt < dt.datetime(2025, 2, 28):
        raise Exception("AIFS data not available before 2025-02-28")

    delta = dt.timedelta(days=1)

    dates = []
    while start_dt <= end_dt:
        dates.append(start_dt)
        start_dt += delta

    # Choose what runs to be downloaded
    runs = ["00", "06", "12", "18"]

    # Choose what steps to be downloaded
    # IFS:
    # - For times 00z &12z: 0 to 144 by 3, 150 to 360 by 6.
    # - For times 06z & 18z: 0 to 144 by 3.
    #
    # AIFS:
    # - 6 hourly steps to 360 (15 days)

    if model == "ifs":
        steps = list(range(0, 121, 3))
    elif model == "aifs":
        steps = list(range(0, 121, 6))

    # Setup loops over dates, runs, steps
    futures = []
    ds_list = []
    for date in dates:
        date_str = date.strftime("%Y%m%d")

        for run in runs:
            date_folder = f"data/ecmwf_{model}/{date_str}_{run}z"
            os.makedirs(date_folder, exist_ok=True)

            for step in steps:
                # Create URL based on the date to be downloaded, using the formats found in the data stores
                url = url_picker(url_base, model, date, run, step)

                grib_file_path = os.path.join(date_folder, f"{model}_{date_str}_{run}z_{step}h.grib2")
                nc_file_path = os.path.join(date_folder, f"{model}_{date_str}_{run}z_{step}h.nc")

                future = dask_client.submit(request_url_process_data, url, grib_file_path, nc_file_path, date_str, run, step)
                futures.append(future)

    wait(futures)