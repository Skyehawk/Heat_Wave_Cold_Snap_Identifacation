
#--- Imports ---
#import sys
from pathlib import Path
#from collections import Counter
from datetime import datetime
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import dask.dataframe as dd

# --- Utility Functions ---

# --- 
#start = timer()


search_path = 'D:\\Libraries\\Documents\\Climatology_Personal\\USCRN_USRCRN_SUBHOURLY\\'
#pattern = '*-MI_Gaylord*.txt'
pattern = '*-Mn_Good*.txt'
#pattern = '*-AK_Fair*.txt'
#pattern = '*-MT_Wolf_Point_29*.txt'
#pattern = '200*_sub_hr\\*-MT_Wolf_Point_29*.txt'
#pattern = '*CRNS0101*.txt'
file_paths = Path(search_path).rglob(pattern)											# Recursively search from path for files matching pattern (with wildcards)

# --- Read in data ---
cols = ['WBANNO','UTC_DATE','UTC_TIME','LST_DATE','LST_TIME','CRX_VN','LONGITUDE','LATITUDE','AIR_TEMPERATURE',
		'PRECIPITATION','SOLAR_RADIATION','SR_FLAG','SURFACE_TEMPERATURE','ST_TYPE','ST_FLAG','RELATIVE_HUMIDITY',
		'RH_FLAG','SOIL_MOISTURE_5','SOIL_TEMPERATURE_5','WETNESS','WET_FLAG','WIND_1_5','WIND_FLAG']
use_cols = ['WBANNO','UTC_DATE','UTC_TIME','LST_DATE','LST_TIME','LONGITUDE','LATITUDE','AIR_TEMPERATURE']

df = dd.read_fwf(list(file_paths), names=cols, usecols=use_cols, na_values=[-9999.0, 999.0, 99.0],	# BE CAREFUL -> the only reason we get away with these na_vals is due to only reading in air temp, if a longitude were to equal a na_val this there would be problems
				parse_dates={'UTC_DATETIME' : ['UTC_DATE', 'UTC_TIME'],
				'LST_DATETIME' : ['LST_DATE', 'LST_TIME']})
df = df.persist()																		# If dataset is small enough we let it persist in system memory
df = df.dropna(subset=['AIR_TEMPERATURE'])												# Remove missing data


temp_thresh = -34.																		# Set a temp threshold to compare against (Celsius)

df['THRESH_ORDINAL_RANK'] = df['AIR_TEMPERATURE'].apply(lambda x: np.sign(x-temp_thresh),	# Generate a column comparing values to threshold: greater (1), lesser (-1), equal (0)
							meta=('AIR_TEMPERATURE', 'float64'))

df['RUN_LENGTH'] = (df.THRESH_ORDINAL_RANK.diff().ne(0)).astype('int').cumsum()			# Find length of runs by offsetting the ordinal ranks, and checking for areas not equal to 0, sum the lengths between those breaks

consecutive_group = df.groupby('RUN_LENGTH')
res = pd.DataFrame({'UTC_BEGIN_DATETIME' : consecutive_group.UTC_DATETIME.first(),
			'UTC_END_DATETIME' : consecutive_group.UTC_DATETIME.last(),
			'LST_BEGIN_DATETIME' : consecutive_group.LST_DATETIME.first(), 
			'LST_END_DATETIME' : consecutive_group.LST_DATETIME.last(),
			'THRESH_ORDINAL_RANK' : consecutive_group.THRESH_ORDINAL_RANK.first(),
			'MEAN_TEMPERATURE' : consecutive_group.AIR_TEMPERATURE.mean().round(1),
			'MAX_TEMPERATURE' : consecutive_group.AIR_TEMPERATURE.max(),
			'MIN_TEMPERATURE' : consecutive_group.AIR_TEMPERATURE.min(),
			'VALS_UTC_DATETIME' : consecutive_group.UTC_DATETIME.agg(list),
			'VALS_TEMPERATURE' : consecutive_group.AIR_TEMPERATURE.agg(list),
			'CONSECUTIVE_RECORDS' : consecutive_group.size()}).reset_index(drop=True)

start = timer()
res['DURATION'] = res['UTC_END_DATETIME'] - res['UTC_BEGIN_DATETIME']					# Calculate the duration of the runs
res = res[res['DURATION'] >= pd.Timedelta("30 minutes")]								# Filter to events 30 minutes and longer

res['VALS_TEMPERATURE_DELTA'] = res['VALS_TEMPERATURE'].map(lambda x: np.diff(x))
res['VALS_UTC_DELTA'] = res['VALS_UTC_DATETIME'].map(lambda x: np.diff(x))

res['TEMP_RATES'] = [[-y/(x.total_seconds()/60.) for y,x in zip(a,b)] for a,b in zip(res['VALS_TEMPERATURE_DELTA'],res['VALS_UTC_DELTA'])]		# Calculate slopes and invert such that negative maps to drop in temp, whereas positive maps to rise in temp
res['MAX_DROP_TEMP_RATE'] = res['TEMP_RATES'].map(lambda x: np.nanmin(x).round(2))	# Find largest delta in rate in values (Drop in temp) units: deg Celsius per minute
res['MAX_RISE_TEMP_RATE'] = res['TEMP_RATES'].map(lambda x: np.nanmax(x).round(2))	# Find largest delta in rate values (Rise in temp)

res['VALS_DEGREE_MINS'] = [[(temp-temp_thresh)*(time.total_seconds()/60.) for temp,time in zip(a,b)] for a,b in zip(res['VALS_TEMPERATURE'],res['VALS_UTC_DELTA'])]
res['DEGREE_MINS'] = res['VALS_DEGREE_MINS'].map(lambda x: np.nansum(x).round(1))	# Find cumulative sum of degree*time spent above or below (basd on heat wave or cold snap)

res.drop(columns=['VALS_UTC_DATETIME', 'VALS_TEMPERATURE', 'VALS_UTC_DELTA', 
					'VALS_TEMPERATURE_DELTA', 'TEMP_RATES', 'VALS_DEGREE_MINS'], inplace=True)	# Remove all temp-series and time-series now that max/min rates are calculated

res_filtered_low = res[res['THRESH_ORDINAL_RANK'] <= 0]									# Seperate the values at or below (above) temp thresh
res_filtered_high = res[res['THRESH_ORDINAL_RANK'] >= 0]

res_filtered_low.drop(columns=['THRESH_ORDINAL_RANK'])									# Drop the ordinal flag of "run is above or below threshold"
res_filtered_high.drop(columns=['THRESH_ORDINAL_RANK'])

res_filtered_low.sort_values(by=['DURATION'], inplace=True)								# Sort those values from shortest run to longest rin
res_filtered_high.sort_values(by=['DURATION'], inplace=True)

end = timer()
print(f'\nTemperature Threshold: {temp_thresh} degrees Celsius')
print('Runs spent at or below threshold:')
print(res_filtered_low.loc[:,'LST_BEGIN_DATETIME':].info())
print(res_filtered_low.loc[:,'LST_BEGIN_DATETIME':].tail(10))
print('\nRuns spent at or above threshold:')
print(res_filtered_high.loc[:,'LST_BEGIN_DATETIME':].info())
print(res_filtered_high.loc[:,'LST_BEGIN_DATETIME':].tail(10))

print(f'Elapsed Time: {end - start}') # Time in seconds