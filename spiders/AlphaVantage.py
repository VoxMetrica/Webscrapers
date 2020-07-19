# -*- coding: utf-8 -*-
import json
import re

import scrapy
import pandas as pd

class AlphavantageSpider(scrapy.Spider):
    name = 'AlphaVantage'
    data = pd.DataFrame()
    tickers = pd.read_csv(r'C:\Users\Graziano\Desktop\Trading\IndexScraper\ftse\FtseCapTable.csv',usecols=[0])['ticker'].tolist()
    tickers.remove('BT.A')
    start_urls = ['https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol='+ticker+'.L&apikey=YTF3PPTVIS9RTDBS' for ticker in tickers][:5]

    def parse(self, response):

        jsonres = json.loads(response.body_as_unicode())
        stonk = re.search('=([A-Z]*).L',response.url).group(1)
        stonk_df = pd.concat([pd.DataFrame(jsonres['Time Series (Daily)']).T],axis=1,keys=[stonk])
        self.data = pd.concat([self.data,stonk_df],axis=1)
        if response.url == self.start_urls[:5][-1]:
            print(self.data)