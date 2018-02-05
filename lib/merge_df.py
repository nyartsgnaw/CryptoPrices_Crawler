import pandas as pd
import os
import sys
#https://medium.com/@agalea91/cryptocompare-api-quick-start-guide-ca4430a484d4
try:
	CWDIR = os.path.abspath(os.path.dirname(__file__))
except:
	CWDIR = os.getcwd()
sys.path.append(CWDIR+'/../lib/utils')

"""
pd.merge(left, right, how='inner', on=None, left_on=None, right_on=None,
         left_index=False, right_index=False, sort=True,
         suffixes=('_x', '_y'), copy=True, indicator=False,
         validate=None)
"""

df_cur = pd.read_excel('{}/../data/input/currency.xlsx'.format(CWDIR))
df_ico = pd.read_excel('{}/../data/input/icorating.xlsx'.format(CWDIR))
df_sym = pd.read_excel('{}/../data/input/coin_symbols.xlsx'.format(CWDIR))


#get all possibility
names_df = pd.merge(df_cur,df_sym,how='outer',left_on='symbol',right_on='Symbol')
#use the name of df_sym


names_df['name_key'] = [str(x).lower() for x in names_df.Name.tolist()]
df_ico['name_key'] = [x.lower() for x in df_ico.name.tolist()]

df = pd.merge(df_ico,names_df,how='left',left_on='name_key',right_on='name_key')



df_hb = pd.read_excel('{}/../data/input/huobi.xlsx'.format(CWDIR))
ls_hb = sum([list(df_hb[x]) for x in df_hb],[])
ls_hb = [x for x in set(ls_hb) if type(x) ==str]

df['huobi_has']=[1 if x in ls_hb else 0 for x in df.Symbol]
df.to_excel('{}/../data/input/attached.xlsx'.format(CWDIR),index=False)

hb_ls = ['EOS','MANA','DAT','POWR','REQ','QSP','CVC','UTK','EVX','TNT','MCO','QTUM','STORJ','BAT','OMG','ACT']
