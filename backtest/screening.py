import shelve
import math
import logging
from datetime import datetime

import constants

def average(s):
    return sum(s) * 1.0 / len(s)

def variance(s):
    avg = average(s)
    return map(lambda x: (x - avg)**2, s)

def stdev(s):
    return math.sqrt(average(variance(s)))

def month_subtract(yyyymm, n):
    start_yyyy = int(yyyymm[:4]) + int((int(yyyymm[-2:]) - n) / 12)
    start_mm = 12 - (n - int(yyyymm[-2:]) - 1) % 12
    start_yyyymm = '%d%02d' % (start_yyyy, start_mm)
    return start_yyyymm

def compute_volatility(security_code, count_months, start_yyyymm, end_yyyymm):
    performances = shelve.open(constants.CACHE_PERFS, 'r')
    security_performances = [performances[security_code][date]
            for date in performances[security_code].keys()
            if date[:6] <= end_yyyymm
            and date[:6] >= start_yyyymm
        ]
    performances.close()
    if len(security_performances) <= 0.8 * (count_months * 20): return None # not enough data
    
    return stdev(security_performances)

def make_volatilities_statistics(universe, count_months, start_yyyymm, end_yyyymm):
    volatilities = dict()
    total = universe.size()
    from multiprocessing import Pool
    pool = Pool(processes=6) 
    results = dict()
    for index, security_code in enumerate(universe.securities()):
        params = [security_code, count_months, start_yyyymm, end_yyyymm]
        results[security_code] = pool.apply_async(compute_volatility, params)
        #logging.debug('volatility for security %s (%d/%d) = %.2f%%' % (security_code, index + 1, total, volatility * 100.0))
        #volatilities[security_code] = compute_volatility(security_code, performances, count_months, start_yyyymm, end_yyyymm)
    
    for security_code in results.keys():
        result = results[security_code].get(timeout=30*60) # max 30 min
        if result:
            volatilities[security_code] = result
    
    return volatilities

class SimpleCache(object):
    
    def __init__(self, cache_name, builder_func, key_builder=str):
        self.__kb = key_builder
        self.__builder = builder_func
        self.__cache_name = cache_name
        
    def get(self, *params):
        cache = shelve.open(self.__cache_name, protocol=2)
        if cache.has_key(self.__kb(*params)):
            instance = cache[self.__kb(*params)]
            
        else:
            instance = self.__builder()
            cache[self.__kb(*params)] = instance
            
        cache.close()
        
        return instance
        
class Screening(object):
    
    def __init__(self, universe):
        self.__performances = shelve.open(constants.CACHE_PERFS, 'r')
        self.__universe = universe
        
    def compute_volatilities(self, yyyymm, count_months, count_securities):
        """
        @TODO: cache results
        """
        end_yyyymm = yyyymm
        start_yyyymm = month_subtract(yyyymm, count_months)
        logging.info('considering volatility over [%s; %s]' % (start_yyyymm, end_yyyymm))
        volatilities = dict()
        
        def stats_builder(universe=self.__universe, cm=count_months, sm=start_yyyymm, em=end_yyyymm):
            return make_volatilities_statistics(universe, cm, sm, em)
        
        cache = SimpleCache(constants.CACHE_SCREENING, stats_builder, key_builder=lambda a, b: str((a, b)))
        volatilities = cache.get(yyyymm, count_months)
        
        logging.info('computed volatility for %d securities' % len(volatilities.keys()))
        
        lowest_volatility = sorted(volatilities, key=volatilities.get)[:count_securities]
        highest_volatility = sorted(volatilities, key=volatilities.get)[-count_securities:]
        logging.debug('lowest volatility: %s' % ([(s, '%.02f%%' % (100.0 * volatilities[s])) for s in lowest_volatility]))
        logging.debug('highest volatility: %s' % ([(s, '%.02f%%' % (100.0 * volatilities[s])) for s in highest_volatility]))
        return (lowest_volatility, highest_volatility)
        