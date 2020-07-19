# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from ..items import FundamentalItem


class FtseFundamentalsSpider(scrapy.Spider):
    name = 'ftse_fundamentals'
    # ticker_info is a table used to create the start urls and used to look up the ticker names using
    # the url extensions.
    ticker_info = pd.read_csv(r'C:\Users\Graziano\Desktop\Trading\IndexScraper\ftse\ftse350_info.csv')

    start_urls = ['https://www.londonstockexchange.com' + str(thing) for thing in
                  ticker_info.fund_link.dropna()]


    def parse(self, response):
        # The web elements of the three tables I'm interested in are saved in in a list.
        tables = response.css('table.table_dati')[:3]

        Item = FundamentalItem()
        # ref saves the url extension so that I can use it to look up the ticker name.
        ref = response.url.split('com')[-1]
        # ticker uses 'ref' to get the ticker name from ticker_info table.
        ticker = self.ticker_info[self.ticker_info.fund_link == ref]['tick'].iloc[0]
        # 'cols' contains the names of the columns of the eventual csv file.
        cols = ['fundamental','first','second','third','fourth','fifth']

        #---------------- Income Statement Table ---------------------#
        # The first thing to add is the headers that give the date of annual statements.
        headers = tables[0].css('th::text').extract()
        # I do a little bit of cleaning.
        headers = [header.replace('\r\n','').replace('\xa0','') for header in headers]#[1:]
        # Now I seperate out the actual dates and the currency they are in...
        # ...I start with the currencies, which are identified by brackets and then cleaned.
        currency = [header.replace('( ','').replace(')','') for header in headers if '(' in header]
        # Now I move on to the dates, which I identify using a dash.
        dates = [header.replace(' ','') for header in headers if '-' in header]

        # I will now write two lines in the csv file. The first line contains the dates. I have to start by
        # putting in the ticker name and the 'Date' fundamental
        Item['ticker'] = ticker
        Item['fundamental'] = 'Date'

        # This for loop sets the dates in headers to the first to fifth columns and finally yields the line.
        for i in range(5):
            Item[cols[1:][i]] = dates[i]
        yield Item

        # I repeat the process but now I input the currency items.
        Item['ticker'] = ticker
        Item['fundamental'] = 'Currency'

        for i in range(5):
            Item[cols[1:][i]] = currency[i]
        yield Item

        # ---------------- Interrupting income table to input industry -------------- #
        Item['ticker'] = ticker
        Item['fundamental'] = 'Industry'

        # I find the industry by searching for 'FTSE ICB sector' and then getting the next td tagged sibling,
        # which should be the actual industry.
        industry = response.xpath('//td[contains(text(), "FTSE ICB sector")]/following-sibling::td/text()')[
            0].extract()

        # I input the industry into every line.
        for i in range(5):
            Item[cols[1:][i]] = industry 
        yield Item
        # ---------------------- back to the income table ------------------------------------- #

        # I move to capturing data in the table body, with 'elements' being a list containing the raw
        # elements of the table
        elements = [element.replace('\r\n', '').replace('\xa0', '') for element in
                    tables[0].css('tbody td::text').extract() if element != '\xa0']

        # reseters contains the sub-headings in the table that will be used to contextualise the
        # fundamental items I record.
        reseters = [element for element in elements if element[-1] == ' ']

        # The for loop works by using reseters as sign posts. The reseter is tacked onto the fundamental
        # and then it counts to five in order to record the line's data. The count to five resets and
        # and goes again until it reaches the next reseter. It yields as it goes.
        count = 0
        for element in elements:
            if element in reseters:
                context = element + ': '
                count = 0
            else:
                if count == 0:
                    Item['ticker'] = ticker
                    Item[cols[count]] = context + element
                    count += 1
                elif count == 5:
                    Item[cols[count]] = element
                    count = 0
                    yield Item
                else:
                    Item[cols[count]] = element
                    count += 1

        # ---------------- Balance Sheet Table ---------------------#

        # I get get all the data from the table into a list called 'elements,' removing all the impurities.
        refined_table = [element for element in tables[1].css('tbody td::text').extract() if element != '\xa0']
        elements = [element.replace('\r\n','').replace('\xa0','') for element in refined_table]
        elements = [element for element in elements if element != '']

        # skip_markers names all the items on the balance sheet that signify that relevant data is on the
        # next line. direct_markers names the items on the balance sheet for which data is on that very line.
        skip_markers = ['other non-current assets','other current assets','other current liabilities',
                        'other non-current liabilities']
        direct_markers = ['total assets','net current assets','total liabilities','net assets']

        # skip_names is the name of the fundamental that I am actually recording when I use the skip_markers.
        skip_names = ['non-current assets','current assets','current liabilities','non-current liabilities']

        # skip_indices and direct_indices finds the index numbers where the balance sheet items in the
        # markers occurs in the elements list. It is used in later for loops to record data.
        skip_indices = [i for i in range(len(elements)) if elements[i].lower() in skip_markers]
        direct_indices = [i for i in range(len(elements)) if elements[i].lower() in direct_markers]

        # This for loop records data from the skip items.
        for i, nami in zip(skip_indices,range(len(skip_indices))):
            Item['ticker'] = ticker
            # skip_names is used in conjuction with the simple nami range iterator to set the fundamental
            # name before the next for loop, which captures the line of data.
            Item['fundamental'] = skip_names[nami].lower()
            # The second for loop uses the i of the super loop to find the start point of the relevant data
            # (which is six ahead of the marker) and loops through five additions.
            for add in range(5):
                Item[cols[1:][add]] = elements[i + 6 + add]
            yield Item

        # I use pretty much the same method for the direct balance sheet items except I don't start six
        # ahead and I use the names of in direct_markers instead of assigning new names.
        for i in direct_indices:
            Item['ticker'] = ticker
            Item['fundamental'] = elements[i]
            for add in range(5):
                Item[cols[1:][add]] = elements[i + 1 + add]
            yield Item

        # The lat item to add is total equity, which doesn't appear in the elements list because of formatting
        # issues it's under a <span> tag rather than a <td> tag. However, it is the last line of the
        # balance sheet, so I just iterate through five from the end of the list.
        Item['ticker'] = ticker
        Item['fundamental'] = 'total equity'
        for i in range(5):
            Item[cols[1:][i]] = elements[i - 5]
        yield Item

        # ---------------- Ratios Table --------------------- #
        # 'elements' once again contains a refined list containing the relevant information.
        elements = [el.css('span::text').extract_first() if el.css('span') != [] else el.css('td::text').
            extract_first() for el in tables[2].css('tbody td')]
        elements = [el.replace('\r\n','').replace('xa0','') for el in elements]
        elements = [el.replace('\xa0','') for el in elements]
        elements = [el for el in elements if el != '']

        # markers works like direct_markers in the balance sheet section.
        markers = ['pe ratio *','peg *','earnings per share growth', 'dividend cover *','revenue per share'
                   'pre-tax profit per share','operating margin','return on capital employed',
                   'dividend yield *','dividend per share growth',
                   'net asset value per share (exc. intangibles)','net gearing *']

        # indices identifies the indices in which markers ocur.
        indices = [i for i in range(len(elements)) if
                   sum([elements[i].lower() in marker for marker in markers]) > 0]

        # This for loop acts like the for loop for direct markers in the balance sheet section.
        for i in indices:
            Item['ticker'] = ticker
            Item['fundamental'] = elements[i]
            for add in range(5):
                Item[cols[1:][add]] = elements[i + 1 + add]
            yield Item












