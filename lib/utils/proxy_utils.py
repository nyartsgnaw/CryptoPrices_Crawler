import json
from bs4 import BeautifulSoup
import requests
import random
import re
import time
import warnings

import os
import sys
try:
	CWDIR = os.path.abspath(os.path.dirname(__file__))
except:
	CWDIR = os.getcwd()



#sys.path.append('{}/../utils'.format(CWDIR))

warnings.filterwarnings('ignore')
"""
def requester_urllib3(url,proxies=None):
	headers = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
	'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0'
	]
	header={'User-Agent':headers[random.randint(0,3)]}

	from urllib3 import ProxyManager, make_headers
	if proxies is not None:
		http = ProxyManager(list(proxies.values())[0],headers = header)
		return http.request('GET', url).data
	else:
		requests.get(url,headers=header,proxies=None).content

def requester_requests(url,proxies=None): #sometimes lose backend bug
	headers = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
	'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
	'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
	'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0'
	]
	header={'User-Agent':headers[random.randint(0,3)]}
	return requests.get(url,headers=header,proxies=proxies).content
"""



def timeout( func, args=(), kwargs={}, timeout_duration=1, default=None):
	import signal

	class TimeoutError(Exception):
		pass

	def handler(signum, frame):
		raise TimeoutError()

	# set the timeout handler
	signal.signal(signal.SIGALRM, handler)
	signal.alarm(timeout_duration)
	try:
		result = func(*args, **kwargs)
	except TimeoutError as exc:
		result = default
	finally:
		signal.alarm(0)
	return result

class proxy_loader(object):

	def __init__(self):
		os.system('mkdir -p {}/proxies/'.format(CWDIR))
		self.addr0 = '{}/proxies/proxies_good'.format(CWDIR)
		self.addr1 = '{}/proxies/proxies_raw1'.format(CWDIR) #proxy_from __get_proxy1
		self.addr2 = '{}/proxies/proxies_raw2'.format(CWDIR) #proxy from __get_proxy2
		self.timeout_duration= 5
		self.proxies_list = []


	def __get_proxy1(self,addr):
		url = 'https://www.hide-my-ip.com/proxylist.shtml'
		html = requests.get(url,headers='')
		soup=BeautifulSoup(html.content)
		tree = soup.find_all('script')
		num_locate = [i for i,x in enumerate(tree) if re.search('<!-- proxylist -->',x.get_text()) is not None][0]
		text = tree[num_locate].get_text()
		with open(addr,'w') as f:
			f.write(text)
		ips= []
		with open(addr,'r') as f:
			for line in f:
				try:
					ip = json.loads(line[:-2])
					print(type(json.loads(line[:-2])))
					ips.append(ip)
				except Exception as e:
					print(e)
		proxies = []
		with open(addr,'w') as f:
			for ip in ips:
				ip_row = ip['tp'].lower()+'://'+ip['i']+':'+ip['p']
				proxies.append(ip_row)
				f.write(ip_row+'\n')
		return proxies

	def __get_proxy2(self,addr):
		url = 'http://www.xicidaili.com/wt/'
		html = requests.get(url,headers='')
		soup=  BeautifulSoup(html.content)
		ips = soup.findAll('tr')
		proxies = []
		with open(addr,'w') as f:
			for i in range(1,len(ips)):
				tds = ips[i].findAll("td")
				ip_row = tds[5].getText().lower()+'://'+tds[1].getText()+':'+tds[2].getText()
				proxies.append(ip_row)
				f.write(ip_row+'\n')
		return proxies

	#'https://hidemy.name/en/proxy-list/'
	#'https://www.hide-my-ip.com/proxylist.shtml'

	def test_proxy(self,proxies_list,timeout_duration=5,test_url='http://www.lrcgc.com/artist-11.html'):
		print('Start testing good_proxies with timeout {}...'.format(timeout_duration))
		print('This will take ~{} seconds, you can skip by providing ./proxies/good_proxies file.'.format(timeout_duration*len(proxies_list)))
		proxies_good=[]
		for _p in proxies_list:
			k = re.findall('^\w*(?=://)',_p)[0] #find http or https
			try:
				start = time.time()
				html = timeout(requests.get,kwargs={'url':test_url,'proxies':{k:_p}},timeout_duration=timeout_duration)
				if html != None:
					gap = time.time() - start
					proxies_good.append(_p)
#					print('Good proxy: {}, latency: {}'.format(_p,gap))
				else:
					pass
#					print('Proxy',_p,'out of time')
			except Exception as e:
				print(e)
		return proxies_good


	def get_good_proxies(self,timeout_duration=5,test_url='http://www.lrcgc.com/artist-11.html'):
		#get proxies_good
		if os.path.isfile(self.addr1):
			with open(self.addr1) as f:
				proxy1 = [x.strip() for x in f.readlines()]
		else:
			proxy1 =self.__get_proxy1(self.addr1)

		if os.path.isfile(self.addr2):
			with open(self.addr2) as f:
				proxy2 = [x.strip() for x in f.readlines()]
		else:
			proxy2 = self.__get_proxy2(self.addr2)

		if os.path.isfile(self.addr0):
			with open(self.addr0) as f:
				proxies_good = [x.strip() for x in f.readlines()]
		else:
			proxies_list = list(set(proxy1+proxy2))
			proxies_good = self.test_proxy(proxies_list=proxies_list, timeout_duration=timeout_duration, test_url=test_url)
			with open(self.addr0,'w') as f:
				for _p in proxies_good:
					f.write(_p+'\n')
			print('Proxy file generated at {}/proxies/proxies_good'.format(CWDIR))

		#change format to proxies_entries
		with open(self.addr0) as f:
			proxies_good = [x.strip() for x in f.readlines()]
		proxies_list = []
		for _p in proxies_good:
			proxies = {}
			k = re.findall('^\w*(?=://)',_p)[0]
			proxies[k]=_p
			proxies_list.append(proxies)
		self.proxies_list = proxies_list
		return proxies_list



	def download_page(self,url,proxies_list=[],timeout_duration=5):
		headers = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:54.0) Gecko/20100101 Firefox/54.0',
		'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
		'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0'
		]
		header={'User-Agent':headers[random.randint(0,3)]}
		start = time.time()

		html = None

		#decide whether to run proxies
		run_proxy = False
		if len(proxies_list) > 0:
			run_proxy = True
		elif len(self.proxies_list)>0:
			run_proxy = True
			proxies_list = self.proxies_list
		#run with proxies?
		if run_proxy == True:
			entries_length = len(proxies_list)
			while html == None:
				proxies = proxies_list[random.randint(0,entries_length-1)]
				print('On proxy: {}'.format(proxies))
				html = timeout(requests.get,kwargs={'url':url,'proxies':proxies,'headers':header},timeout_duration=timeout_duration)
				if (time.time() - start) >timeout_duration*3:
					break

		if html ==None:
			print('Without proxy')
			html=requests.get(url,proxies=None)

		if (time.time()-start) < timeout_duration/2:
			sleep_time = random.randint(int(timeout_duration/3),int(timeout_duration/2))
			time.sleep(sleep_time)
		print('Downloading takes {} secs'.format(time.time()-start))
		return html


def initializer(timeout_duration=5,test_url='http://www.lrcgc.com/artist-11.html'):
	pl1 = proxy_loader()
	pl1.get_good_proxies(timeout_duration=timeout_duration,test_url=test_url)
	download_page = pl1.download_page
	return download_page

if __name__ == '__main__':
	#initializing proxies directory...
	initializer()
