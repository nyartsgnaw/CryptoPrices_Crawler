import os
import sys
#https://medium.com/@agalea91/cryptocompare-api-quick-start-guide-ca4430a484d4
#https://www.cryptocompare.com/api/
import pandas as pd
import requests

try:
	CWDIR = os.path.abspath(os.path.dirname(__file__))
except:
	CWDIR = os.getcwd()
sys.path.append('{}/utils'.format(CWDIR))
from utils import  price_historical, update_price_df
from multitask_utils import multi_work

os.system('mkdir -p {}/../data/prices/day'.format(CWDIR))
os.system('mkdir -p {}/../data/prices/hour'.format(CWDIR))
os.system('mkdir -p {}/../data/prices/minute'.format(CWDIR))
#loop_ls = ['ETH','USD','QSP','MANA','EOS']
#hb_ls = ['EOS','ACT','MANA','DAT','POWR','REQ','QSP','CVC','UTK','EVX','TNT','MCO','QTUM','STORJ','BAT','OMG']
def get_prices(coin,CWDIR):
#	results={}
	for period in ['day','hour','minute']:
		try:
			data = price_historical(coin,'BTC',period)
			if data.shape[0]==0:
				print('Get 0 data for {}'.format(coin))
				return [coin]
			addr_file ='{}/../data/prices/{}/Price_{}_by{}_byBTC.csv'.format(CWDIR,period,coin,period)
			acc_data = update_price_df(addr_file,data)
		except Exception as e:
			print(e)
			print('Failed on {} by {}...'.format(coin,period))
			acc_data = 'Failed'
			#logged the failed terms
			return [coin]
#		results[coin,period]=acc_data
	print('{} done.'.format(coin))
	return

if __name__ == '__main__':

	''' ******************* user input  **************************** '''
	ON_DISK = True
	scaling_number = 16
	''' ******************* user input  **************************** '''
	"""
	url = 'https://www.cryptocompare.com/api/data/coinlist/'
	data = requests.get(url).json()
	loop_ls = list(data['Data'].keys())
	df = pd.DataFrame(list(data['Data'].values()))

	for i in range(len(loop_ls)):
		if loop_ls[i]!=df.loc[i,'Symbol']:
			print(i,loop_ls[i],df.loc[i,'Symbol'])

	df.to_csv('./../data/input/cryptocompareCoinList.csv',index=False)
	df_sym = pd.read_excel('{}/../data/input/coin_symbols.xlsx'.format(CWDIR))
	loop_ls = df_sym.Symbol.tolist()

	"""

	df_sym = pd.read_csv('{}/../data/input/cryptocompareCoinList.csv'.format(CWDIR))
	loop_ls = df_sym.Symbol.tolist()

	outs = multi_work(thelist=list(enumerate(loop_ls)),func=get_prices,arguments=[[CWDIR]],scaling_number=scaling_number,on_disk=ON_DISK)
