import logging
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

class Universe(object):
    
    def __init__(self, securities):
        self.__preinit_securities = securities
        logging.info('preinitialized a universe of %d securities' % len(securities))
        
        self.__liquidity_data = defaultdict(lambda : defaultdict(int))
        with open('stats-volume.db', 'r') as liquidity_file:
                liquidity_data_lines = [row.split(',') for row in map(str.strip, liquidity_file.readlines()) if len(row) != 0]
                for quarter_start, quarter_end, code, volume in liquidity_data_lines:
                    if volume > 0:
                        self.__liquidity_data[quarter_start] [code] = int(volume)
        
        self.__securities = list()
        self.__initialized = False
        
    def init_month(self, year, month, min_dollar_volume):
        logging.info('initializing universe with liquid securities for month %d-%02d' % (year, month))
        self.__securities = list() # resets list of securities
        as_of_date = datetime(year, month, 1)
        yyyymm = (as_of_date - timedelta(days=63)).strftime('%Y%m')
        quarters = {
            '01': '01', '02': '01', '03': '01', 
            '04': '04', '05': '04', '06': '04',
            '07': '07', '08': '07', '09': '07', 
            '10': '10', '11': '10', '12': '10',
            }
        quarter = yyyymm[:4] + quarters[yyyymm[4:]]
        logging.info('selection based on data from quarter: %s' % quarter)
        for code in self.__liquidity_data[quarter].keys():
            if code not in self.__preinit_securities: continue
            volume =  self.__liquidity_data[quarter][code]
            if volume >= min_dollar_volume:
                self.__securities.append(code)
        
        self.__initialized = True
        
    def securities(self):
        assert self.__initialized, 'Universe has not been initialized'
        return self.__securities
        
    def size(self):
        assert self.__initialized, 'Universe has not been initialized'
        return len(self.__securities)
