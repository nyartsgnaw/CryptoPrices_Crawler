# CryptoPrices_Crawler

The code can
1. retrieve coin prices by day/hour/minute from https://www.cryptocompare.com/api/ , and
2. extract finance features like movingAverage/RSI/change/exchanges from retrieved data.  

### Usage   
1. Data retrieving: ```python ./lib/get_prices.py ```
   Find features at ```./data/features/```
2. Feature extracting: ```python ./lib/get_features.py```  
   Find pricecs at ```./data/prices/```
