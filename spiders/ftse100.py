# -*- coding: utf-8 -*-
import scrapy
from ..items import FtseItem

class Ftse100Spider(scrapy.Spider):
    name = 'ftse100'
    # I have commented out the pipeline in the settings section and added it here.
    custom_settings = {
        'ITEM_PIPELINES' : {
            'ftse.pipelines.FtsePipeline': 300
        }
    }
    start_urls = [
        'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=UKX',
        'https://www.londonstockexchange.com/exchange/prices-and-markets/stocks/indices/summary/summary-indices-constituents.html?index=MCX'
        ]

    def parse(self, response):
        # I do not start by creating an item instance because I must do that in the get_mktcap function, else if I
        # declare an item and fill in its fields, then pass it on to get_mktcap, then the for loop will be completed
        # before get_mktcap starts parsing the requests, meaning that it thinks every stock is the last stock on the
        # lse page. Instead I start by creating triples of ticker, name, and links to the description of the stock.
        zipper = zip(response.xpath("//td[@scope]//text()").extract(), response.css(".name a::text").extract(),
                     response.css(".name a::attr(href)").extract())

        # I identify whether the stock is from the ftse100 or ftse250 using the response url
        index_url = response.url.split('&')[0][-3:]
        if index_url == 'UKX':
            index = 'ftse100'
        elif index_url == 'MCX':
            index = 'ftse250'
        else:
            print("I can't tell which index this is")
            index = 'unclear'

        # I now start a for loop in which I pass each link as a request, passing the other characteristics to the
        # get_mktcap function through the 'meta' argument. I pass them as a dictionary. I do not pass an item for the
        # reasons stated above.
        for ticker, name, link in zipper:
            request = scrapy.Request(response.urljoin(link), meta={'ticker':ticker,'name':name,'index':index},
                                     callback=self.get_mktcap)
            yield request

        # I progress through the pagination by identifying and following the next button until it does not exist anymore
        next_buttons = response.xpath("//a[@title='Next']//@href")
        if next_buttons.get() is None:
            pass
        elif len(next_buttons) == 2:
            yield response.follow(next_buttons.get(), callback=self.parse)
        else:
            yield response.follow(next_buttons.get(), callback=self.parse)
            print('there are not two buttons')

    def get_mktcap(self,response):
        # I create the Item container
        item = FtseItem()

        # I use the meta container to recover information about the
        ticker = response.meta['ticker']
        item['name'] = response.meta['name']
        item['index'] = response.meta['index']

        # I clean up the name of the ticker before committing it to the item container
        if '.' in ticker and ticker.split('.')[-1] == '':
            item['tick'] = ticker.split('.')[0]
        else:
            item['tick'] = ticker

        # item['name'] = name
        # item['index'] = index

        # I find the market cap on the page and commit it to the item container.
        item['mktcap'] = response.xpath("//td[contains(text(),'£m*')]/following-sibling::td[@class='name']/text()").get()

        # I also include the relative link to the fundamentals.
        item['fund_link'] = response.xpath("//a[@title='Fundamentals']/@href").extract_first()

        yield item


