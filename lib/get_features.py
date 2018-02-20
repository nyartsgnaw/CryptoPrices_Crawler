import pandas as pd
import datetime
import re
import time
pd.options.mode.chained_assignment = None  # default='warn'
from utils.utils import time_it,read_price_csv
import os
import sys
try:
	CWDIR = os.path.abspath(os.path.dirname(__file__))
except:
	CWDIR = os.getcwd()
sys.path.append('{}/utils'.format(CWDIR))
from multitask_utils import multi_work

os.system('mkdir -p {}/../data/prices/day'.format(CWDIR))
os.system('mkdir -p {}/../data/prices/hour'.format(CWDIR))
os.system('mkdir -p {}/../data/prices/minute'.format(CWDIR))

class coin_price_df(object):
	@time_it
	def __init__(self,coin_name,period):
		self.coin_name = coin_name.upper()
		self.period = period
		self.exchange_names = ['BTC']
		try:
			self.df_raw = read_price_csv('./../data/prices/{}/Price_{}_by{}_byBTC.csv'.format(period,coin_name,period))
			self.df_raw['timestamp']=self.df_raw.loc[:,'timestamp'].astype('datetime64[ns]')
		except Exception as error:
			print(error)
		self.df = self.df_raw.loc[:,['timestamp','volumeto','close']]
		self.df.columns = ['timestamp','volumeto','price_BTC']

	@time_it
	def add_exchanges(self,df):
		#read exchanges price pair
		self.df_ETH = read_price_csv('./../data/prices/{}/Price_{}_by{}_byBTC.csv'.format(self.period,'ETH',self.period))
		self.df_ETH['timestamp']=self.df_ETH.loc[:,'timestamp'].astype('datetime64[ns]')
		self.df_USD = read_price_csv('./../data/prices/{}/Price_{}_by{}_byBTC.csv'.format(self.period,'USD',self.period))
		self.df_USD['timestamp']=self.df_USD.loc[:,'timestamp'].astype('datetime64[ns]')

		#find the common earliest time and common latest time
		self.time_0, self.time_n = min(self.df_raw.timestamp),max(self.df_raw.timestamp)
		self.time_0_ETH,self.time_n_ETH = min(self.df_ETH.timestamp),max(self.df_ETH.timestamp)
		self.time_0_USD,self.time_n_USD = min(self.df_USD.timestamp),max(self.df_USD.timestamp)
		self.common_min_time = max(self.time_0,self.time_0_ETH,self.time_0_USD)
		self.common_max_time = min(self.time_n,self.time_n_ETH,self.time_n_USD)

		#get the exchanges
		df = df.loc[(df['timestamp']>=self.common_min_time)&(df['timestamp']<=self.common_max_time),:]
		self.df_ETH = self.df_ETH.loc[(self.df_ETH['timestamp']>=self.common_min_time)&(self.df_ETH['timestamp']<=self.common_max_time),:]
		df['price_ETH'] = df['price_BTC'].values/self.df_ETH['close'].values
		self.df_USD = self.df_USD.loc[(self.df_USD['timestamp']>=self.common_min_time)&(self.df_USD['timestamp']<=self.common_max_time),:]
		df['price_USD'] = df['price_BTC'].values/self.df_USD['close'].values
		self.exchange_names = [re.sub('price_','',x) for x in sum([re.findall('price.*',x) for x in df.columns],[])]
		return df

	@time_it
	def to_rec_df(self,df,gap_unit=3600,window_size=1):
		df = df.copy()
		gap = datetime.timedelta(seconds=gap_unit)
		left_edge = self.common_min_time
		right = self.common_max_time
		left = right
		outs = []
		while left >= left_edge:
			left = right - gap * window_size
			judge = (df['timestamp']<=right) & (df['timestamp']>left)
			rows = df.loc[judge,:]

			def avg_indexes(df_rows,exchange_names=['BTC','ETH','USD']):
				mean_volume = df_rows['volumeto'].mean()
				mean_prices = df_rows.loc[:,['price_'+x for x in exchange_names]].mean()
				if df_rows.shape[0]>1:
					std_prices = df_rows.loc[:,['price_'+x for x in exchange_names]].std()
					std = list(std_prices/mean_prices)
				else:
					std=[0]*mean_prices.shape[0]
				return mean_volume,mean_prices,std

			mean_volume,mean_prices,std = avg_indexes(rows,exchange_names = self.exchange_names)

			merged_row =[right,mean_volume]+list(mean_prices)+list(std)
			outs.append(merged_row)
			right = right - gap
		df = pd.DataFrame(outs[::-1],columns=['timestamp','volumeto']+['price_'+x for x in self.exchange_names]+['std_'+x for x in self.exchange_names])
		return df

	@time_it
	def add_changeRate(self,df,window_size=1):
		#legitimate window_size >= 2

		df = df.copy()
		for name in self.exchange_names:
			changes = [1]*window_size
			i = window_size
			while i < df.shape[0]:
				right = df['price_'+name].iloc[i]
				left = df['price_'+name].iloc[i-window_size:i-1].mean()
				change = (right-left)/left
				changes.append(change)
				i+=1
			df['change_'+name] = changes
		return df

	@time_it
	def add_RSI(self,df,window_size=1):
		#legitimate window_size >= 1
		df = df.copy()
		def get_gain_loss(i,df,window_size):
			# legitimate i >= window_size
			prev_close = df.iloc[i-1]['volumeto']
			rows = df.iloc[i-window_size:i]
			current_gain = rows.loc[rows['volumeto']>=prev_close,'volumeto'].sum()
			current_loss = rows.loc[rows['volumeto']<prev_close,'volumeto'].sum()
			if current_loss == 0:
				current_loss = 0.00001
			return current_gain, current_loss

		i = window_size
		current_gain,current_loss = get_gain_loss(i,df,window_size)
		avg_gain = current_gain/window_size
		avg_loss = current_loss/window_size

		RSI = 50
		RSIs = [RSI]*window_size
		while i <df.shape[0]:
			current_gain,current_loss = get_gain_loss(i,df,window_size)
			avg_gain=(avg_gain * (window_size-1) + current_gain)/window_size
			avg_loss=(avg_loss * (window_size-1) + current_loss)/window_size

			RS= avg_gain/avg_loss
			RSI = 100 - 100/(1+RS)
			RSIs.append(RSI)
			i+=1
		df['RSI'] = RSIs
		return df


def featurize_price(coin,period,CWIDR):
	try:
		hh_df = coin_price_df(coin,period)
		df1 = hh_df.add_exchanges(hh_df.df)
		df2 = hh_df.to_rec_df(df1,gap_unit=3600,window_size=1)
		df3 = hh_df.add_changeRate(df2,window_size=2)
		df4 = hh_df.add_RSI(df3,window_size=4)
		df4.to_excel('{}/../data/features/{}/Features_{}_by{}.xlsx'.format(CWDIR,period,coin,period),index=False)
	except Exception as error:
		print(error)
	return

if __name__ == '__main__':
	''' ******************* user input  **************************** '''
	scaling_number = 16
	ON_DISK = True
	''' ******************* user input  **************************** '''

	df_sym = pd.read_csv('{}/../data/input/cryptocompareCoinList.csv'.format(CWDIR))
	loop_ls = df_sym.Symbol.tolist()
#	loop_ls = ['EOS','ETH','USD','MANA','QSP']
	period = 'hour'

	outs = multi_work(thelist=list(enumerate(loop_ls)),func=featurize_price,arguments=[[period,CWDIR]],scaling_number=scaling_number,on_disk=ON_DISK)
