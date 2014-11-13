import os

DATASOURCE_DIR = 'datasources'
CACHE_DIR = 'cache.db'

PRICES_DATA = os.sep.join((DATASOURCE_DIR, 'us-prices-adjusted-1992-2014.zip'))
UNADJUSTED_PRICES_DATA = os.sep.join((DATASOURCE_DIR, 'us-prices-unadjusted-1992-2014.zip'))

SOURCE_US_EQUITIES = os.sep.join((DATASOURCE_DIR, 'us-equities.csv'))
SOURCE_US_FUNDS = os.sep.join((DATASOURCE_DIR, 'us-funds.csv'))
SOURCE_DIVIDENDS = os.sep.join((DATASOURCE_DIR, 'dividends.csv'))
SOURCE_BENCHMARK = os.sep.join((DATASOURCE_DIR, 'sptr.csv'))

CACHE_PERFS = os.sep.join((CACHE_DIR, 'perf-data.db'))
CACHE_VOLUMES = os.sep.join((CACHE_DIR, 'stats-volume.db'))
CACHE_SCREENING = os.sep.join((CACHE_DIR, 'tmp-cache-screening.db'))
