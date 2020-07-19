# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

#------- Here is my note----------
# consider removing the 'return item' part of the pipeline, since I don't think it's really necessary

import sqlite3

class FtsePipeline(object):

    def __init__(self):
        self.create_connection()
        self.create_table()

    def create_connection(self):
        self.conn = sqlite3.connect("FtseTickers.db")
        self.curr = self.conn.cursor()

    def create_table(self):
        self.curr.execute("""DROP TABLE IF EXISTS ticker_table100""")
        self.curr.execute("""create table ticker_table100(
                        ticker text,
                        name text,
                        ind text,
                        market_cap text,
                        fundamentals_link text)""")

    def process_item(self, item, spider):
        self.store_db(item)
        print(f'In my pipeline I have {item["name"]} with the ticker {item["tick"]}')
        return item

    def store_db(self, item):
        self.curr.execute("""insert into ticker_table100 values (?,?,?,?,?)""",(
            item["tick"],
            item["name"],
            item["index"],
            item["mktcap"],
            item["fund_link"]
        ))
        self.conn.commit()
