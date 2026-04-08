import requests
import zipfile


import datetime as dt

type Date = tuple[int,int,int]

def validate_dates(dates:list[Date]) -> list:
	'''
	Sanity check to see if dates exist and are available from the subwaydata.nyc page

	Current earliest date is April 1, 2021. Latest date is the previous day, limiting to 2 days prior.
	'''
	invalid = []
	latest = (dt.datetime.now() - dt.timedelta(2)).date()
	earliest = dt.date(2021,4,1)

	for date in dates:
		try:
			dt.date(date[0],date[1],date[2])
			if not (earliest <= dt.date(date[0],date[1],date[2]) <= latest):
				invalid.append(date)
		except ValueError:
			invalid.append(date)
			continue
	if invalid:
		raise ValueError(f"Found invalid dates: {invalid}")
	else: return dates


def get_current_gtfs(path:str="gtfs/gtfs_files/"):
    '''Download Regular GTFS Static files from MTA website and unzip'''
    URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
    r = requests.get(URL)
    with open("gtfs/gtfs.zip", "wb") as fd:
        fd.write(r.content)
    with zipfile.ZipFile("gtfs/gtfs.zip","r") as zip_ref:
        zip_ref.extractall(path)
        
    return

def get_old_gtfs():
    '''Download old schedules from mta open data on socrata, using their API. Not implemented yet.'''
    return

def get_subwaydata():
    '''Get the desired data files from subwaydata.nyc'''
    for month,day,year in dates:
        file_name = f"subwaydatanyc_{year}-{month:02}-{day:02}_csv.tar.xz"
        url = f"https://subwaydata.nyc/data/{file_name}"
        response = requests.get(url)
        with open(file_name, "wb") as f:
            f.write(response.content)
            
    return