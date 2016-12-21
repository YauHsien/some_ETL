from wic import default as Default
from wic.util import logging
from wic.util.logging import logger
from dateutil import parser as DateParser
import sys, traceback



def cast(obj, filename, linum, column, value, to_type, mod_name= __name__):
    return value
        
    #if type(value) is None:
    #    return None
        
    #if to_type in set([str, 'varchar']):
    #    return value

    #if to_type in set([int, 'int', 'tinyint', 'bigint']):
    #    return int(value)

    #if to_type in set(['date', 'datetime']):
    #    return DateParser.parse(value)

    #logger(mod_name).error('unknown type "{}": value "{}"'.format(to_type, value))
    #sys.exit(Default.INTERRUPT)
