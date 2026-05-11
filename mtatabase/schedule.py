from __future__ import annotations
import pandas as pd
import re
import numpy as np
from src.mtatabase.utils import zip_reduce
from typing import Any

class Schedule:
	'''GTFS Schedule class that has a dataframe with every trip and their scheduled stops plus arrival times'''
	def __init__(self, gtfs_folder:str='gtfs/gtfs_files'):
		'''
		Params:
			schedule (str): Filepath to the GTFS Static file folder 
		'''
		self.gtfs_folder = gtfs_folder
		self._raw_trips = pd.read_csv(f'{gtfs_folder}/trips.txt')
		self.trips = self._preprocess(self._raw_trips)
		self.duplicate_ids: dict[str, Any] = {}
		self.lookup = self._create_lookup()
		self.start_date, self.end_date = [[d[:4], d[4:6], d[6:]] for d in pd.read_csv('gtfs/gtfs_files/calendar.txt')[["start_date","end_date"]].astype(str).values[0].tolist()]
		return
	
	def _preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
		'''Since the MTA doesn't have any semble
		nce of consistent formatting, the arrival log format is also messed up.
		This method generates a new column of IDs formatted the same way as the arrival trip files which allow for joins later on.
		'''
		def _get_shapes(trips: pd.DataFrame) -> pd.Series:
			'''fix possible nonexistent shape_ids'''
			# will always match since the trip_id field is constructed in a way that it ends with a shape id regardless of format
			new_shape_id = trips.trip_id.map(lambda s: re.search(r"_..?.\.[NS](?:.)*",str(s)).group()[1:]) # type: ignore 
			return new_shape_id
		

		trips = raw.copy()
		trips['shape_id'] = _get_shapes(trips)
		new_ids = zip_reduce(trips.route_id.to_list(), trips.shape_id.to_list(), lambda id, shape: '.'.join([id, re.split(r"\.",shape,maxsplit=1)[1]]))
		times = trips.trip_id.map(lambda s: str(s).split('_')[-2]).to_list()
		
		# replace route ids in trip id with actual ID for 7X, W, FX, Z but not 6X for whatever reason
		short_id_raw = pd.Series(zip_reduce(times, new_ids, lambda t, id: '_'.join([t,id]))).map(lambda s: re.sub('6X','6',s)) # type: ignore
		
		# fix staten island dot format
		trips['short_id'] = short_id_raw.map(lambda s: re.sub(r"SI\.", "SI",s)).map(lambda s: re.sub(r"SS\.", "SS",s)) # type: ignore

		return trips
	
	def _create_lookup(self):
		'''Creates a unique ID and lookup table since we have to reverse engineer some shape IDs due to duplicate routes/times but starting offset. GTFS does not have a unique ID for real time arrival data so we have to get clever (or jank). This is necessary for linking to the arrival logs, as the shape ID is optional for realtime GTFS.'''

		srd = pd.read_csv(f'{self.gtfs_folder}/stop_times.txt')
		schedcopy = self.trips.copy()
		id_key = schedcopy[['trip_id','shape_id']]

		# we always have x.values since x is generated from dataframe column grouping
		shape_map = srd.groupby('trip_id').apply(lambda x: [list(w) for w in np.transpose(x.values)][:3]).reset_index() # type: ignore
		shape_map.columns = ['trip_id','stop_list']


		tr_map = pd.DataFrame.from_records(shape_map.stop_list, columns = ['stop','arr','dep'])
		tr_map.insert(0, 'trip_id',shape_map.trip_id)
		joined = schedcopy.join(tr_map.set_index('trip_id'), on='trip_id').set_index('trip_id')
		lookup = id_key.join(joined.drop(['shape_id'],axis=1), on='trip_id', lsuffix='_')

		# we know the regex exists again here since the shape ID always exists
		lookup['tiny_id'] = lookup.short_id.map(lambda s: re.search(r"(.)*_..?.\.[NS]",str(s)).group()) # type: ignore
		lookup.drop(['trip_headsign','direction_id'],axis=1, inplace=True)

		# get duplicated IDs for short_ID matching
		lk = lookup.copy()
		for day_type in ['Weekday','Saturday','Sunday']:
			lkw = lk[lk.service_id == f'{day_type}'].reset_index()
			dupes = lkw[lkw.tiny_id.duplicated()]
			self.duplicate_ids[day_type] = dupes.tiny_id.values
		

		return lk

	

	def __getitem__(self, key):
		return self.trips[key]
	
	
