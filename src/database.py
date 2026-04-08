from __future__ import annotations
import pandas as pd
import re
from src.utils import zip_reduce
import glob
import datetime

class Schedule:
	'''GTFS Schedule class'''
	def __init__(self, gtfs_folder:str='gtfs/gtfs_files'):
		'''
		Params:
			schedule (str): Filepath to the GTFS Static file folder 
		'''
		self._raw_trips = pd.read_csv(f'{gtfs_folder}/trips.txt')
		self.trips = self._preprocess(self._raw_trips)
		return
	
	def _preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
		'''Since the MTA doesn't have any semblence of consistent formatting, the arrival log format is also messed up.
		This method generates a new column of IDs formatted the same way as the arrival trip files which allow for joins later on.
		'''
		def _get_shapes(trips: pd.DataFrame) -> pd.Series:
			'''fix possible nonexistent shape_ids'''
			# will always match since the trip_id field is constructed in a way that it ends with a shape id regardless of format
			new_shape_id = trips.trip_id.map(lambda s: re.search(r"_..?.\.[NS](?:.)*",str(s)).group()[1:]) # type: ignore 
			return new_shape_id
		

		trips = raw.copy()
		trips['shape_fix'] = _get_shapes(trips)
		new_ids = zip_reduce(trips.route_id.to_list(), trips.shape_fix.to_list(), lambda id, shape: '.'.join([id, re.split(r"\.",shape,maxsplit=1)[1]]))
		times = trips.trip_id.map(lambda s: str(s).split('_')[-2]).to_list()

		# this regex will always match since we forced the shape_id to exist in a prior step.
		trips['short_id'] = pd.Series(zip_reduce(times, new_ids, lambda t, id: '_'.join([t,id]))).map(lambda s: re.search(r"(?:.)*\.+[NS]",s).group()) # type: ignore

		

		return trips
	
	def __getitem__(self, key):
		return self.trips[key]
	
	def __repr__(self) -> str:
		return self.trips.to_string(index=False)
	
	

class LoggedTrips:
	'''Class for the subway arrival log trips files'''
	def __init__(self, data_path: str):
		'''
		Params:
			data_path (str): Path to the folder that contains the desired `trips.csv` files from subwaydata.nyc. Use `gtfs_script.get_subwaydata` to download these.
		'''
		def _aggregate(data_path: str = data_path) -> tuple[pd.DataFrame, list]:
			'''combines trips.csv files into a single database'''
			
			all_files = glob.glob(f'data/{data_path}/*_trips.csv')
			df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
			dates = [f.split('_')[-2] for f in all_files]
			df.drop_duplicates(inplace=True)
			return df, dates
		
		
		self._raw_trips, self.dates = _aggregate(data_path)
		self.trips = self._preprocess(self._raw_trips)
		return

	def _preprocess(self, raw_trips: pd.DataFrame):
		'''Convert timestamp into date-tuples with short ids'''
		def _create_date_id(s:str):
			time, shape = s.split('_')
			dt = datetime.datetime.fromtimestamp(float(time))
			return [dt, shape]
		trips = raw_trips.copy()
		new_df =  pd.DataFrame(map(_create_date_id, trips.trip_uid), columns=['datetime', 'shape_id'])
		return pd.concat([trips,new_df],axis=1)
	

	def __getitem__(self, key):
		return self.trips[key]
	def __repr__(self) -> str:
		return self.trips.to_string(index=False)

class TripDB:
	'''Class for the database of scheduled and actual times'''

	def __init__(self):

		return