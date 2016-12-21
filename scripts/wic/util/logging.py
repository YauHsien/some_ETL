from wic import RESTRICT
from wic import default as Default
import logging
import sys

def logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO \
                    if RESTRICT.PLATFORM in ['PRODUCTION', 'DISTRIBUTION', 'RELEASE'] \
                    else logging.DEBUG)
    return logger

def config(format):
    logging.basicConfig(format= format)


"""
## ====================================================
## Build a progress bar
##
## like following:
## [==#-------] %(n) / %(m) MBs %(notation)
##
"""

def switch_to_progress(logger_name):
    h = logging.getLogger().handlers[0]
    h.terminator = ''
    h.setFormatter(logging.Formatter('%(message)s'))
    logging.info('')

def switch_to_normal(logger_name):
    logger(__name__).info('\r')
    h = logging.getLogger().handlers[0]
    h.terminator = '\n'
    #h.setFormatter(logging.Formatter(''))
    #logging.info('\n')
    h.setFormatter(logging.Formatter(Default.LOG_FMT))

def put_progress(n, m, annotation):
    n1 = int(n / m * 10)
    m1 = 9 - n1
    if n1 <= 0:
        logger(__name__).info('\r[>---------] {}'.format(annotation))
    elif m1 <= 0:
        logger(__name__).info('\r[=========>] {}'.format(annotation))
    else:
        logger(__name__).info('\r[{}>{}] {}'.format('=' * n1, '-' * m1, annotation))

_rolling_bar_symbols = dict([x for x in zip([n for n in range(8)], ['⣟⣾', '⡿⣽', '⣷⣽', '⣯⡿', '⢿⣻', '⣷⣟', '⢿⣾', '⣻⣯']) ])
# '⣾⣽⣻⢿⡿⣟⣯⣷'
# ' ░▒▓█▓▒░'
# ['⣟⣾', '⡿⣽', '⣷⣽', '⣯⡿', '⢿⣻', '⣷⣟', '⢿⣾', '⣻⣯']
        
def put_rolling_bar(n, m, annotation):
    logger(__name__).info('\r{} {}'.format( _rolling_bar_symbols[int(n / m) % 8], annotation ))
    

"""
##
## [==#-------] %(n) / %(m) MBs %(notation)
## like above:
##
## Build a progress bar
## ====================================================
"""
