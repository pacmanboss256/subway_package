from __future__ import annotations
import pandas as pd
import re
import numpy as np
from utils import zip_reduce, TripDate
import textwrap


class LoggedDay:
	'''Class for a single day of subway arrival files'''
	def __init__(self, trip_path: str, stop_path: str):
		'''
		Params:
			trip_path (str): path to subwaydata.nyc `*_trips.csv` file
			stop_path (str): path to subwaydata.nyc `*_stop_times.csv` file
		'''
		
		self.raw_trips = pd.read_csv(trip_path)
		# tuple unpack shenanigans
		self.date = TripDate(*[int(f) for f in trip_path.split('_')[-2].split('-')])
		self.day_type = self.date._daytype
		self.trips = self._preprocess(self.raw_trips)
		self.lookup = self._merge(self.trips, stop_path)
		return

	def _preprocess(self, raw_trips: pd.DataFrame) -> pd.DataFrame:
		'''Set up IDs to match formatting, see `src/schedule.py` for more'''
		trips = raw_trips.copy()
	
		new_ids = zip_reduce(trips.route_id.to_list(), trips.trip_id.to_list(), lambda id, shape: '.'.join([id, re.split(r"\.",shape,maxsplit=1)[1]]))
		trips['short_id'] = pd.Series(zip_reduce(trips.trip_id.map(lambda s: str(s).split('_')[-2]).to_list(), new_ids, lambda t, id: '_'.join([t,id]))).map(lambda s: re.sub('6X','6',s) if '6X' in s else s).map(lambda s: re.sub('SS','SI',s) if 'SS' in s else s) # type: ignore
		# remove trips on end time of day since they are in the next day anyways
		trips = trips[trips.start_time.between(self.date.start_time, self.date.end_time, inclusive='left')]
		def tiny_exists(r):
			'''sanity check'''
			try: 
				return re.search(r"(.)*_..?.\.[NS]",str(r)).group() # type: ignore
			except AttributeError:
				print(r)
				raise AttributeError

		trips['tiny_id'] = trips.short_id.map(lambda s: tiny_exists(s)) # type: ignore
		trips['start_time_formatted'] = trips.vehicle_id.map(lambda s: re.split(r"( \d+\+? ?)",s)[1].strip()).map(lambda t: ':'.join(textwrap.wrap(str(t).ljust(6,'0').replace('+','3'),2))) # type: ignore
		return trips

	def _merge(self, trip: pd.DataFrame, stop: str) -> pd.DataFrame:
		'''Join on stop times'''
		stoplog = pd.read_csv(stop, dtype=str)

		stoplog['arrival_time'] = stoplog['arrival_time'].fillna(stoplog['departure_time'])

		## keep consistent formatting between the two, we can melt later
		tmap = stoplog.groupby('trip_uid').apply(lambda x: np.array([list(w) for w in np.transpose(x.values)[:-2]])).reset_index() # type: ignore
		tmap.columns = ['trip_uid','data']
		
		trip = trip.copy().merge(tmap, on='trip_uid')
		ldex = pd.DataFrame(trip.data.explode().values.reshape(len(trip.data),4), columns=['stops','track','arr_time','dep_time']) # type: ignore
		return pd.concat([trip.drop('data',axis=1), ldex], axis=1)
	

	def __getitem__(self, key):
		return self.trips[key]
	
	def __repr__(self) -> str:
		return self.trips.to_string() 


