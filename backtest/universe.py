import logging
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

class Universe(object):
    
    def __init__(self, securities):
        self.__preinit_securities = securities
        logging.info('preinitialized a universe of %d securities' % len(securities))
        
        self.__liquidity_data = defaultdict(lambda : defaultdict(int))
        with open('stats-volume.csv', 'r') as liquidity_file:
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
        
#
#
#

class SecurityNotFoundError:
    pass

class UniverseOld(object):
    
    def load_prices(self, yyyymm_start, yyyymm_end):
        SECURITIES_DATA = 'us-equities.csv'
        PRICES_DATA = 'us-prices-equities-1992-2014.zip'
        self.__db = sqlite3.connect(':memory:')
        self.__db.execute("""CREATE TABLE IF NOT EXISTS security
            (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            code_quandl VARCHAR(64) UNIQUE NOT NULL
            )"""
            )
        self.__db.execute("""CREATE TABLE IF NOT EXISTS quote 
        (
        as_of_date DATE NOT NULL,
        security_id INT NOT NULL,
        last DECIMAL(10,5) NOT NULL,
        FOREIGN KEY(security_id) REFERENCES security(id),
        PRIMARY KEY (as_of_date, security_id)
        )
        """)
        with open(SECURITIES_DATA) as secfile:
            securities = map(lambda line: line.strip(), secfile.readlines())
            for row in securities[1:]:
                (quandl_code, bbg_code, isin, name) = row.split(',')
                if quandl_code not in self.__preinit_securities:
                    #logging.debug('code %s not in universe' % quandl_code)
                    continue
                    
                if not bbg_code.startswith('#N/A'):
                    self.__db.execute("""INSERT OR IGNORE INTO security 
                        (code_quandl) 
                        VALUES (?)""", 
                        (quandl_code))
                    self.__securities.append(quandl_code)
       
        self.__db.commit()
                
        count = self.__db.execute('SELECT COUNT(*) FROM SECURITY').fetchone()[0]
        logging.info('loaded profiles for %d securities from file %s' % (count, SECURITIES_DATA))
        
        #logging.debug('securities: %s' % )
        
        with ZipFile(PRICES_DATA, 'r') as prices:
            for index, security_code in enumerate(self.all_securities()):
                batch_count = index / 100 + 1
                if index % 100 == 0:
                    logging.info('processing batch %d/%d' % (batch_count, len(self.all_securities()) / 100 + 1)) 
                
                if security_code in self.all_securities():
                    dataset_name = 'output/%s.txt' % security_code
                    dataset = prices.open(dataset_name).readlines()
                    #logging.debug('imported prices for %s' % security_code)
                    self.import_security(dataset, yyyymm_start, yyyymm_end, security_code)                
                    self.__db.commit()
                    
        logging.info('excluded %d securities' % len(self.get_excluded()))
                            
        self.__initialized = True
    
    def import_security(self, dataset, yyyymm_start, yyyymm_end, security_code):
        #logging.debug('importing security prices %s' % security_code)
        sec_id = self.__db.execute("""SELECT id FROM security WHERE code_quandl=?""", (security_code, )).fetchone()
        if sec_id:
            oldest_date = datetime.strptime(dataset[0].strip().split(',')[0], '%Y-%m-%d')
            if oldest_date.strftime('%Y%m') > yyyymm_start:
                self.exclude(security_code)
                
            for row in dataset:
                items = row.strip().split(',')
                px_date = datetime.strptime(items[0], '%Y-%m-%d')
                if px_date.strftime('%Y%m') < yyyymm_start or px_date.strftime('%Y%m') > yyyymm_end:
                    continue
                            
                px_last = items[4]
                
                self.__db.execute("""INSERT OR IGNORE INTO quote
                    (as_of_date, security_id, last) 
                    VALUES (?, ?, ?)""", (px_date, sec_id[0], px_last))
                
        else:
            logging.error('failed to load data for security "%s"' % security_code)
            raise SecurityNotFoundError
    
    def exclude(self, code):
        self.__excluded.append(code)
    
    def get_excluded(self):
        return self.__excluded
    
    def get_performance(self, as_of_date, code):
        """
        Dividend adjusted performance.
        """
        assert self.__initialized, 'Universe prices has not been initialized'
        dividends = self.get_dividends(as_of_date, as_of_date, code)
        cur_price = self.get_price(as_of_date, code) + dividends
        prev_price = self.get_price(as_of_date - timedelta(days=1), code)
        return  cur_price / prev_price - 1.0
        
    def get_dividends(self, date_start, date_end, code):
        """
        TODO: queries db
        Returns sum of dividends received, boundary dates included.
        """
        assert self.__initialized, 'Universe prices has not been initialized'
        return 0.0

    def get_prices(self, code):
        prices = self.__db.execute("""
            SELECT
                q.as_of_date, q.last 
            FROM quote q
                JOIN security s on s.id = q.security_id
            WHERE
                s.code_quandl=?
            """, (code, )).fetchall()
            
        return prices
            
        
    def get_price(self, as_of_date, code):
        """
        """
        assert self.__initialized, 'Universe prices has not been initialized'
        
        # finds latest date before date
        price_date = self.__db.execute("""
            SELECT
                MAX(q.as_of_date) 
            FROM
                quote q
                JOIN security s on s.id = q.security_id
            WHERE
                s.code_quandl=?
                AND q.as_of_date <= ?
            """, (code, as_of_date)).fetchone()
        
        if not price_date[0]:
            logging.error('no price as of %s for %s' % (as_of_date, code))
            prices_range = self.__db.execute("""
                SELECT
                    MIN(q.as_of_date), MAX(q.as_of_date) 
                FROM
                    quote q
                    JOIN security s on s.id = q.security_id
                WHERE
                    s.code_quandl=?
                """, (code, )).fetchone()
            logging.error('available track is %s' % str(prices_range))
            
        #logging.debug('found price date %s for %s on as of date %s' % (price_date[0], code, as_of_date))
            
        # use that date to get the price
        price_last = self.__db.execute("""
            SELECT
                q.last 
            FROM quote q
                JOIN security s on s.id = q.security_id
            WHERE
                s.code_quandl=?
                AND q.as_of_date = ?
            """, (code, price_date[0])).fetchone()
                        
        return price_last[0]

    def all_securities(self):
        return self.__securities
        
    def size(self):
        assert self.__initialized, 'Universe prices has not been initialized'
        return len(self.__securities)
