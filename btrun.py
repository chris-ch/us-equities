import os
import sys
import logging
from datetime import datetime
from datetime import timedelta
import calendar

from backtest.universe import Universe
from backtest.screening import Screening
from backtest.pricing import Pricing
from backtest import constants

def month_range(start_yyyymm, count=10, step=3):
    yyyymm = start_yyyymm
    year = int(yyyymm[:4])
    month = int(yyyymm[-2:])
    for i in xrange(count):
        first_date = datetime(year, month, 1)
        for j in xrange(step):
            last_day = calendar.monthrange(year, month)[1]
            second_date = datetime(year, month, last_day)
            year = (second_date + timedelta(days=1)).year
            month = (second_date + timedelta(days=1)).month
            
        next_range = (first_date, second_date)
        yield next_range
        
def normalized(percents):
    total = float(sum(percents.values()))
    final = dict()
    for code in percents.keys():
        final[code] = float(percents[code]) / total
    
    return final
    
def apply_strategy(pricer, percents, amount, as_of_date):
        """
        Buy/Sell at close price.
        """
        weights = normalized(percents)
        portfolio = dict()
        final_amount = amount
        for code in weights.keys():
            position = weights[code] * amount
            #
            # buying / selling shares
            #
            price = pricer.get_price(as_of_date, code)
            shares = position / price
            portfolio[code] = int(round(shares))
            final_amount -= float(portfolio[code]) * price
            
        return portfolio, final_amount
    
def create_portfolio(pricer, amount, securities, as_of_date):
    logging.info('creating portfolio as of %s' % as_of_date.strftime('%Y-%m-%d'))
    weightings = dict()
    for security in securities:
        # assumes equal weighting
        # TODO: adjust weighting according to individual beta
        weightings[security] = 1.0 / len(securities)
    
    portfolio, residual_cash = apply_strategy(pricer, weightings, amount, as_of_date)
    return portfolio, residual_cash
   
class Backtest(object):
    
    def __init__(self):
        self.__pricer = Pricing() 
    
    def run_period(self, date_start, date_end, portfolio, residual_cash):
        dividends = dict()
        for code in portfolio.keys():
            dividends[code] = self.__pricer.get_dividends(date_start, date_end, code) * float(portfolio[code])
        
        residual_cash += sum(dividends.values())
        
        return residual_cash

    def turn_shares_into_amounts(self, portfolio, as_of_date, normalized=False):
        """
        Using as_of_date close price.
        """
        amounts = dict()
        total = 0.0
        for code in portfolio.keys():
            price = self.__pricer.get_price(as_of_date, code)
            amount = portfolio[code] * price
            amounts[code] = amount
            total += amount
            
        if normalized:
            for code in amounts.keys():
                amounts[code] /= total
        
        return amounts
    
    def delta_additions(self, portfolio, prev_portfolio):
        codes = set(portfolio.keys())
        prev_codes = set(prev_portfolio.keys())
        added_codes = codes - prev_codes
        additions = [(code, portfolio[code]) for code in added_codes]
        return additions
    
    def delta_deletions(self, portfolio, prev_portfolio):
        codes = set(portfolio.keys())
        prev_codes = set(prev_portfolio.keys())
        dropped_codes = prev_codes - codes
        deletions = [(code, prev_portfolio[code]) for code in dropped_codes]
        return deletions
    
    def delta_adjustments(self, portfolio, prev_portfolio):
        codes = set(portfolio.keys())
        prev_codes = set(prev_portfolio.keys())
        adjusted_codes = codes & prev_codes
        adjustments = [(code, portfolio[code] - prev_portfolio[code]) for code in adjusted_codes]
        return adjustments
    
    def turnover(self, date, portfolio, prev_portfolio):
        prev_amounts = self.turn_shares_into_amounts(prev_portfolio, date)
        amounts = self.turn_shares_into_amounts(portfolio, date)
        dropped_amount = 0.0
        for code in dropped_codes:
            logging.debug('liquidation of %s: %.0f' % (code, prev_amounts[code]))
            dropped_amount += prev_amounts[code]
            
        added_amount = 0.0
        for code in added_codes:
            logging.debug('new investment in %s: %.0f' % (code, amounts[code]))
            added_amount += amounts[code]
            
        adjusted_amount = 0.0
        for code in adjusted_codes:
            adjustment = amounts[code] - prev_amounts[code]
            logging.debug('adjustment for %s: %.0f' % (code, adjustment))
            adjusted_amount += adjustment
            
        return (dropped_amount, added_amount, adjusted_amount)

    def valuation(self, date, portfolio, cash):
        return sum( self.turn_shares_into_amounts(portfolio, date).values() ) + cash
        
    def positions(self, date, portfolio):
        return self.turn_shares_into_amounts(portfolio, date)
        
    def get_benchmark_performance(self, start_date, end_date):
        price_start = self.__pricer.get_benchmark_level(start_date)
        price_end = self.__pricer.get_benchmark_level(end_date)
        return price_end / price_start - 1.0

def main():
    with open(constants.SOURCE_US_EQUITIES, 'r') as equities_file:
        equities = [row.split(',')[0] for row in map(str.strip, equities_file.readlines())[1:]]
        
    universe = Universe(equities)
    screener = Screening(universe)
    pricer = Pricing() 
    bt = Backtest()
    portfolios = dict()
    prev_portfolio = dict()
    cash = 1e6
    amount_invested = cash # initial investment
    
    for date_start, date_end in month_range('200801', 2, 3):
        logging.info('creating portfolio as of %s' % (date_start.strftime('%Y-%m-%d')))
        universe.init_month(date_start.year, date_start.month, 10e6)
        logging.info('universe size: %d' % universe.size())
        
        hist_data_range = date_start - timedelta(days=1)
        (buy_list, sell_list) = screener.compute_volatilities(hist_data_range.strftime('%Y%m'), count_months=18, count_securities=100)
        
        logging.info('investing %.0f as of %s' % (amount_invested, date_start.strftime('%Y-%m-%d')))
        portfolio, residual_cash = create_portfolio(pricer, amount_invested, buy_list, date_start)
        logging.debug('additions %s' % bt.delta_additions(portfolio, prev_portfolio))
        logging.debug('deletions %s' % bt.delta_deletions(portfolio, prev_portfolio))
        logging.debug('adjustments %s' % bt.delta_adjustments(portfolio, prev_portfolio))
        # records positions
        portfolios[date_start] = (portfolio, residual_cash)
            
        logging.info('back testing over period %s through %s' % (date_start.strftime('%Y-%m-%d'), date_end.strftime('%Y-%m-%d')))
        final_cash = bt.run_period(date_start, date_end, portfolio, residual_cash)
        amount_final = bt.valuation(date_end, portfolio, final_cash)
        logging.info('valuation as of %s: %.0f' % (date_end.strftime('%Y-%m-%d'), amount_final))
        logging.debug('positions at start of period: %s' % (bt.turn_shares_into_amounts(portfolio, date_start)))
        logging.debug('positions at end of period: %s' % (bt.turn_shares_into_amounts(portfolio, date_end)))
        logging.info('performance / benchmark: %.2f%% / %.2f%%' % ((amount_final / amount_invested - 1.0) * 100.0, bt.get_benchmark_performance(date_start, date_end)  * 100.0))
        amount_invested = amount_final
        prev_portfolio = portfolio
            
    logging.info('finished backtesting')
    
if __name__ == '__main__':
    # goal is to generate an output of portfolio performances
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s %(asctime)s %(module)s - %(message)s',
        filename=sys.argv[0].split('.')[0]  + '.log', filemode='w'
    )
    logging.getLogger('btrun').setLevel(logging.DEBUG)
    logging.getLogger('screening').setLevel(logging.INFO)
    main()
    