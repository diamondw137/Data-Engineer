import pandas as pd
import json
from pandas.io.json import json_normalize
import numpy as np
import time as time
import requests
from datetime import datetime, timedelta

from itertools import zip_longest

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def getTimes():
	start = datetime.utcnow() - timedelta(hours=2)
	start = time.mktime(start.timetuple())
	start = str(int(start))

	end = datetime.utcnow() - timedelta(hours=1)
	end = time.mktime(end.timetuple())
	end = str(int(end))
	return (start, end)

def GetDataForSites(f, KeyId):

	sites = pd.read_csv(f)
	sites = sites[['UID',"Project: Project Name", "Site ID"]]
	site_list = sites['UID'].dropna().values

	start, end = getTimes()

	url = 'https://api.engie.com:8065/wss/v2/weather/forecast'
	headers = { "KeyId" : KeyId, "content-type" : "application/json" }

	dfs = []
	for sites in grouper(site_list, int(len(site_list) / 50)):
		sites = [x for x in sites if x is not None]
	
		payload = {
			"start": start,
			"end": end,
			"timeStep" : 3600,
			"params": ['absolute_humidity_2m:gm3','clear_sky_rad:W','dew_or_rime:idx','dew_point_2m:C','diffuse_rad:W','diffuse_rad_1h:Ws','direct_rad:W','direct_rad_1h:Ws','dust_0p03um_0p55um:ugm3','dust_0p55um_0p9um:ugm3','dust_0p9um_20um:ugm3','effective_cloud_cover:p','fresh_snow_1h:cm','frost_depth:cm','global_rad:W','global_rad_1h:Ws','high_cloud_cover:p','is_fog_1h:idx','is_rain_1h:idx','is_sleet_1h:idx','is_snow_1h:idx','low_cloud_cover:p','medium_cloud_cover:p','neff:p','pm1:ugm3','pm10:ugm3','pm2p5:ugm3','precip_1h:mm','prob_precip_1h:p','relative_humidity_2m:p','sfc_pressure_mean_1h:hPa','snowdepth:cm','snow_melt_1h:mm','sunrise:sql','sunset:sql','sunshine_duration_1h:min','t_0m:C','t_2m:C','t_max_0m_1h:C','t_mean_0m_1h:C','t_min_0m_1h:C','total_cloud_cover:p','wet_bulb_t_2m:C','wind_dir_10m:d','wind_dir_mean_10m_1h:d','wind_gusts_10m:ms','wind_speed_10m:ms','wind_speed_mean_10m_1h:ms','wind_speed_u_10m:ms','wind_speed_v_10m:ms'],
			"targetId": sites
		}

		response = requests.post(url, headers = headers, json = payload)
		data = json.loads(response.text)
		myDataFrame = ParseDataFromMeteomatics(data)
		dfs.append(myDataFrame)

	df = pd.concat(dfs, axis=1)
	return df


def ParseDataFromMeteomatics(d):
	# Get TargetIDs
	targetIds = json_normalize(d, record_path = ["targetIds"])
	targetIds.columns = ['targetIds']

	# Get Coords
	coords = json_normalize(d)
	coords = coords[["coordinate.latitude", "coordinate.longitude"]]

	# Get dayData
	dayData = json_normalize(d, record_path = ["dayDatas"], meta=["targetIds"])
	dayData = dayData[["day", "timeStep", "targetIds"]]

	# Get params
	params = json_normalize(d, record_path = ["dayDatas", "parameters"], meta=["targetIds"])

	data = pd.concat([targetIds, coords], axis=1)
	data = data.join(dayData.set_index("targetIds"), on="targetIds")
	data = data.join(params.set_index("targetIds"), on="targetIds")
	data = data.reset_index(drop=True)
	data = data.explode('value')
	data["rn"] = data.groupby(by=['targetIds', 'day', 'name'])['coordinate.latitude'].rank(method='first')
	data['datetime'] = pd.to_datetime(data['day']) + pd.to_timedelta(data['rn'] - 1, unit='h')
	data = data[['targetIds', 'coordinate.latitude', 'coordinate.longitude', 'name', 'datetime', 'value']]
	data = data.reset_index(drop=True)
	return data;

myDf = GetDataForSites('Solar_Sites.csv', KeyId = '8cd4919a-5f26-4495-b305-838c9ae9568b')
myDf.to_csv('output.csv', index=False)