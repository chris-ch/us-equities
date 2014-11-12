import sqlite3
import os
import logging
from zipfile import ZipFile
from collections import defaultdict
from operator import itemgetter

from backtest import constants

def median(values):
    sorts = sorted(values)
    length = len(sorts)
    if not length % 2:
        return (sorts[length / 2] + sorts[length / 2 - 1]) / 2.0
    return sorts[length / 2]
    
class InconsistentDateOrder:
    pass

def main():
    quarters = { 
        '01': ('01', '03'),
        '02': ('01', '03'),
        '03': ('01', '03'),
        '04': ('04', '06'),
        '05': ('04', '06'),
        '06': ('04', '06'),
        '07': ('07', '09'),
        '08': ('07', '09'),
        '09': ('07', '09'),
        '10': ('10', '12'),
        '11': ('10', '12'),
        '12': ('10', '12'),
    }
    prices_source = constants.UNADJUSTED_PRICES_DATA
    with ZipFile(prices_source, 'r') as prices_zip, open('stats-volume.db', 'w') as stats_file:
        data_files = prices_zip.namelist()
        for index, dataset_name in enumerate(data_files):
            batch_count = index / 100 + 1
                            
            if index % 100 == 0: logging.debug('processing batch %d/%d' % (batch_count, len(data_files) / 100 + 1)) 
                    
            dataset = prices_zip.open(dataset_name).readlines()
            security_code = dataset_name.split('/')[1][:-4] # forward slash required by zip spec
            volumes = defaultdict(list)
            prev_date = '1970-01-01'
            for row in dataset:
                fields = row.strip().split(',')
                if fields[0] <= prev_date:
                    raise InconsistentDateOrder
                    
                else:
                    prev_date = fields[0]
                    
                if fields[-2].startswith('#N/A') or fields[-1].startswith('#N/A'):
                    continue
                
                month = fields[0][5:7]
                quarter_start, quarter_end = quarters[month]
                date_start = fields[0][:4] + quarter_start
                date_end = fields[0][:4] + quarter_end
                volume = float(fields[-1]) * float(fields[-2])
                volumes[(date_start, date_end)].append(volume)
            
            for date_start, date_end in sorted(volumes.keys(), key=itemgetter(0)):
                median_volume = int(median(volumes[(date_start, date_end)]))
                row = (date_start, date_end, security_code, median_volume)
                stats_file.write(','.join(map(str, row)))
                stats_file.write(os.linesep)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s %(asctime)s %(module)s %(message)s'
    )
    main()
    