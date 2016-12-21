import wic
from wic import RESTRICT
from wic.RESTRICT import (CM, FM, PM, PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW)
from wic.util.logging import logger
from wic.util import file as File
import sys, datetime, zipfile, csv, re
"""
## This module is for DB_COLUMNS_.+[.]zip processing.
"""

_all_PM_cases = [PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW]

"""
## get owner-table-columns dictionary:
## (owner, table name) => column name => column information
"""
def get_ow_ta_columns(zippath, filename, tmppath, CAT):
    if File.is_path(tmppath):
        result = dict()
        for dic in _gen_ow_ta_columns(zippath, filename, tmppath, CAT):
            ot = (_encode(dic['OWNER']), _encode(dic['TABLE_NAME']))
            if ot not in result:
                result[ot] = dict()
            columns = result[ot]
            c =       _encode(dic['COLUMN_NAME'])
            columns[c] = dict()
            col =        columns[c]
            col['type'] =      _encode(dic['DATA_TYPE'])
            col['len'] =       int(_encode(dic['DATA_LENGTH']))
            col['precision'] = _try_int(_encode(dic['DATA_PRECISION']))
            col['scale'] =     _try_int(_encode(dic['DATA_SCALE']))
            col['order'] =     int(_encode(dic['COLUMN_ID']))
        return result

def _gen_ow_ta_columns(zippath, filename, tmppath, CAT):
    zf = zipfile.ZipFile(str(zippath))
    zf.extract(filename, str(tmppath))
    zf.close()
    with open(str(tmppath.joinpath(filename)), 'r', newline= '\r\n', encoding= 'latin-1') as fo:
        reader = csv.DictReader(fo, delimiter= RESTRICT.DELIMITER)
        for d in reader:
            if _in_category(d, CAT):
                yield d
                        
def _in_category(dic, cat):
    if CM == cat:
        return dic['OWNER'] == 'CMDLTE'
    elif FM == cat:
        return dic['OWNER'] == 'FM' and dic['TABLE_NAME'] == 'FX_ALARM'
    elif PM == cat:
        return dic['OWNER'] in set(_all_PM_cases)
    else:
        return cat in set(_all_PM_cases) and dic['OWNER'] == cat

def _encode(value):
    return str(value.encode(), sys.getdefaultencoding())

def _try_int(value):
    if type(value) is str:
        try:
            return int(value)
        except:
            return None
