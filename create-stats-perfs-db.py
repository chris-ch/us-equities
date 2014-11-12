from collections import defaultdict
from zipfile import ZipFile
from datetime import datetime
from itertools import izip
import logging
import sys
import shelve

from backtest import constants

def main():
        PRICES_DATA = constants.PRICES_DATA
        performances = shelve.open('perf-data.db', protocol=2)
       
        with ZipFile(PRICES_DATA, 'r') as prices_data:
            securities = prices_data.namelist()
            for index, dataset_name in enumerate(securities):
                #if index == 100: break
                batch_count = index / 100 + 1
                if index % 100 == 0:
                    logging.info('processing batch %d/%d' % (batch_count, len(securities) / 100 + 1)) 
                
                security_code = dataset_name.split('/')[-1][:-4]
                security_performances = dict()
                dataset = prices_data.open(dataset_name).readlines()
                dates = list()
                prices = list()
                for row in dataset:
                    items = row.strip().split(',')
                    px_date = datetime.strptime(items[0], '%Y-%m-%d')
                    if items[4].startswith('#N/A'):
                        continue
                        
                    px_last = float(items[4])
                    dates.append(px_date)
                    prices.append(px_last)

                for date, price, price_prev in izip(dates[1:], prices[1:], prices[:-1]):
                    perf = (price  / price_prev) - 1.0
                    security_performances[date.strftime('%Y%m%d')] = perf
                    
                performances[security_code]  = security_performances
                
        performances.close()
        
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s %(asctime)s %(module)s - %(message)s'
    )
    main()
    