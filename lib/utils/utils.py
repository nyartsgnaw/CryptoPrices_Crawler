import requests
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import time
import os

def time_it(f):
    def wrapper(*args,**kwargs):
        start = time.time()
        x = f(*args,**kwargs)
        end = time.time()
        print('Function {} took {} seconds to run.'.format(f.__name__,str(end-start)))
        return x
    return wrapper

def read_price_csv(file_loc):
	df = pd.DataFrame(columns=['close', 'high', 'low', 'open', 'time', 'volumefrom', 'volumeto','timestamp'])

	datatypes = {'close':float,'high':float, 'low':float, 'open':float, 'time':int, 'volumefrom':float, 'volumeto':float,'timestamp':str}
	try:
		#dtype={"user_id": int, "username": object},
		df = pd.read_csv(file_loc,engine='c',na_filter=False,warn_bad_lines=False,error_bad_lines=False
						,dtype=datatypes
						)
	except:
		try:
			def read_csv_robust(file_loc):
#				print('Reading with robust csv_reader... for {}'.format(file_loc))
				with open(file_loc, "r",encoding='utf-8') as f:
					reader = csv.reader(f, delimiter=",")
					lines = []
					for i, line in enumerate(reader):
						if len(line) == len(datatypes):
							lines.append(line)
					df = pd.DataFrame(lines[1:],columns=lines[0])
				for k,v in datatypes.items():
					df[k] = df[k].astype(v)
				return df
			df = read_csv_robust(file_loc)
			# df = pd.read_csv(file_loc,engine='python',na_filter=False,warn_bad_lines=False,error_bad_lines=False,dtype= datatypes)
		except Exception as error:
						pass
#			print('Failed to read {}, the error message is:'.format(file_loc))
#			print(error)

	df['timestamp'] = df.loc[:,'timestamp'].astype('datetime64[ns]')

	df = df.sort_values('timestamp')

	return df

#page = requests.get('https://min-api.cryptocompare.com/data/histoday?fsym=BTC&tsym=ETH&limit=100000&aggregate=1')

#page1= requests.get('https://min-api.cryptocompare.com/data/pricehistorical?fsym=ETH&tsyms=BTC,USD,EUR&ts=0')
#page2= requests.get('https://min-api.cryptocompare.com/data/pricemultifull?fsyms=ETH,DASH&tsyms=BTC,USD,EUR')


def check_limits():
	output = {}
	for period in ['hour','minute','second']:
		url= 'https://min-api.cryptocompare.com/stats/rate/{}/limit'.format(period)
		page = requests.get(url)
		limit = page.json()['CallsLeft']['Histo']
		output[period]=limit
	return output


"""

{'Aggregated': False,
 'Data': [],
 'MaxLimits': {'Hour': 8000, 'Minute': 300, 'Second': 15},
 'Message': 'Rate limit excedeed!',
 'Response': 'Error',
 'Type': 99,
 'YourCalls': {'hour': {'Histo': 8979},
  'minute': {'Histo': 1},
  'second': {'Histo': 1}}}

	#sleep or not

{'Aggregated': False,
 'ConversionType': {'conversionSymbol': '', 'type': 'direct'},
 'Data': [],
 'FirstValueInArray': True,
 'Response': 'Success',
 'TimeFrom': 1438905600000,
 'TimeTo': 1517270400,
 'Type': 100}

period = 'day'
period0 = 'histoday'
symbol = 'ETH'
comparison_symbol='BTC'
limit = 10000000000
aggregate=1
exchange=''
all_data = True




"""

def price_historical(symbol, comparison_symbol, period='hour', limit=10000000, aggregate=1, exchange='',all_data=True):
	time.sleep(1)
	if period =='day':
		period0 = 'histoday'
	elif period =='hour':
		period0 = 'histohour'
	elif period =='minute':
		period0 = 'histominute'

	url = 'https://min-api.cryptocompare.com/data/{}?fsym={}&tsym={}&limit={}&aggregate={}'\
			.format(period0, symbol.upper(), comparison_symbol.upper(), limit, aggregate)
	if exchange:
		url += '&e={}'.format(exchange)
	if all_data:
		url += '&allData=true'

	page = requests.get(url)
	while page.json()['Type']==99:
		print('Return type 99')
		called_hour = page.json()['YourCalls']['hour']['Histo']
		called_minute = page.json()['YourCalls']['minute']['Histo']
		called_second = page.json()['YourCalls']['second']['Histo']
		if called_hour >= 8000:#https://min-api.cryptocompare.com/stats/rate/hour/limit
			print('Start waiting for 1700s')
			time.sleep(60)
		if called_minute >=300:#https://min-api.cryptocompare.com/stats/rate/minute/limit
			print('Start waiting for 50s')
			time.sleep(1)
		if called_second >= 15:#https://min-api.cryptocompare.com/stats/rate/second/limit
			print('Start waiting for 1s')
			time.sleep(1)

		page = requests.get(url)

	_data = page.json()['Data']
	df = pd.DataFrame(_data)
	if df.shape[0]>0:
		df['timestamp'] = [datetime.datetime.fromtimestamp(d) for d in df.time]
	else:
		df=pd.DataFrame(columns=['close', 'high', 'low', 'open', 'time', 'volumefrom', 'volumeto','timestamp'])


#        print('Get 0 data for {} by {}'.format(symbol,period))
	return df


def update_price_df(_addr,_data):
	hist_price_df = read_price_csv(_addr)
	if hist_price_df.shape[0] >0:
		hist_latest_time = max(hist_price_df['timestamp'])
	else:
		hist_latest_time = np.datetime64('1970-01-01T00:00:00Z')
	diff = _data.loc[_data['timestamp'] > hist_latest_time,:]
	new_df = pd.concat([hist_price_df,diff],axis=0)
	del hist_price_df
	new_df.to_csv(_addr,index=False)
	return new_df
