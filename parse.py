import pandas as pd
import json
from pandas.io.json import json_normalize

with open('response.json') as f:
    d = json.load(f)

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

print(data)
data.to_csv('response.csv')