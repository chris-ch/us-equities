from zipfile import ZipFile
from datetime import datetime

def find_latest_before(as_of_date, prices):
    dates = prices.keys()
    max_date = max([date for date in dates if date <= as_of_date])
    return prices[max_date]

class Pricing(object):
    
    def __init__(self):
        self.__prices_zip = ZipFile('us-prices-unadjusted-1992-2014.zip', 'r')
        with open('sptr.csv', 'r') as benchmark_file:
            self.__benchmark = dict()
            for row in benchmark_file.readlines():
                yyyymmdd, value = row.strip().split(',') 
                self.__benchmark[datetime.strptime(yyyymmdd, '%Y-%m-%d')] = float(value)
                            
        with open('dividends.csv', 'r') as dividends_file:
            self.__dividends = dict()
            for row in dividends_file.readlines():
                code, yyyymmdd, value = row.strip().split(',') 
                if not self.__dividends.has_key(code):
                    self.__dividends[code] = dict()
                    
                self.__dividends[code][datetime.strptime(yyyymmdd, '%Y%m%d')] = float(value)
            
        self.__prices = dict()
        
    def get_dividends(self, date_start, date_end, code):
        values = [self.__dividends[code][date]
            for date in self.__dividends[code].keys()
            if date >= date_start and date <= date_end]
            
        return sum(values)
    
    def get_benchmark_level(self, date):
        return find_latest_before(date, self.__benchmark)
    
    def get_price(self, as_of_date, code):
        if not self.__prices.has_key(code):
            self.__prices [code] = dict()
            with self.__prices_zip.open('output/' + code + '.txt') as prices_file:
                for row in prices_file.readlines():
                    fields = row.strip().split(',')
                    date = datetime.strptime(fields[0], '%Y-%m-%d')
                    if not fields[-2].startswith('#N/A'):
                        self.__prices[code] [date] = float(fields[-2])
            
        prices = self.__prices [code]
        return find_latest_before(as_of_date, prices)