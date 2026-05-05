from functools import reduce
from typing import Callable
import numpy as np
import datetime
from zoneinfo import ZoneInfo
from typing import Literal

### Types and Constants

UTC = ZoneInfo("UTC")

DayType = Literal['Weekday','Saturday','Sunday']


### Classes

class TripDate:
	'''quick object for timestamp boundaries and trip date'''
	def __init__(self, year:int, month:int, day:int):
		self.year = year
		self.month = month
		self.day = day
		self.start_time = int(datetime.datetime(year,month,day).timestamp())
		self.end_time = int((datetime.datetime(year,month,day) + datetime.timedelta(1)).timestamp())
		self._daytype = get_DayType(self.date())
		
	def date(self) -> datetime.datetime:
		'''return datetime object'''
		return datetime.datetime(self.year,self.month,self.day)

	def __repr__(self) -> str:
		return self.date().strftime('%Y-%m-%d')


### Functions

def get_DayType(date: str|datetime.datetime) -> DayType:
	'''convert datetime object into service_id day type'''
	if isinstance(date, str):
		date = datetime.datetime.strptime(date,'%Y-%m-%d')
	
	match date.weekday():
		case 6:
			return 'Sunday'
		case 5:
			return 'Saturday'
		case _:
			return 'Weekday'
		


def zip_reduce(a: list, b: list, f: Callable) -> list:
	'''Zip two lists and reduce by a function'''
	return list(map(lambda _: reduce(f, _), zip(a,b)))



def timestr_to_mta(t: str) -> str:
	'''Convert a string "HH:MM:SS" into hundredths of a minute past midnight'''
	lt = np.array([int(j) for j in t.split(':')])
	hdm = sum((lt * np.array([60,1,1/60]))*100)
	return str(int(hdm)).rjust(6,'0')


def timestamp_to_mta(ts: int) -> str:
	'''Convert a timestamp value into MTA time (which is in hundredths of a minute past midnight relative to UTC)'''
	t = datetime.datetime.fromtimestamp(ts, tz=UTC).time()
	htm = int((t.hour*60 + t.minute + t.second/60)*100)
	return str(htm).rjust(6,'0')
	