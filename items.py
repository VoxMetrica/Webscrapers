# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class FtseItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    tick = scrapy.Field()
    name = scrapy.Field()
    index = scrapy.Field()
    mktcap = scrapy.Field()
    fund_link = scrapy.Field()

class FundamentalItem(scrapy.Item):
    ticker = scrapy.Field()
    fundamental = scrapy.Field()
    first = scrapy.Field()
    second = scrapy.Field()
    third = scrapy.Field()
    fourth = scrapy.Field()
    fifth = scrapy.Field()