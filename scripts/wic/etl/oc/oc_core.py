from wic import RESTRICT
from wic.etl import common as Common
from wic.etl import column
from wic.etl import db as DB
from wic.etl import extraction
from wic.util.logging import logger
import sys, re, datetime



_cat = RESTRICT.OC
_re_occ_filename = re.compile('^C_LTE_(CTP|UTP)_COMMON_OBJECT_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]txt$')


"""
# LRC:          (lambda x: LRCn)(y)
# OBJ_GID:      extraction.etract('CO_GID')
# TIME:         --
# STATUS:       --
"""
def _build_occ_extraction(filename, column, col_type, prefix, LRCn):

    if column in set(['LRC']):
        return lambda obj, obj1, line, linum: LRCn
    
    elif column == 'OBJ_GID':
        return lambda obj, obj1, line, linum:\
            extraction.cast(obj1, filename, linum, column, Common.lnval(line, 'CO_GID'), col_type, __name__)




def get_occ_ex_header(LRC, filename, cusid= cusid, tech, CAT= _cat):
    colfpath = column.find_file_path(cusid, tech, CAT)
    cols = DB.get_columns(cusid, tech, CAT, mod_name= __name__)
    prefix = Common.get_prefix_by_LRC(LRC)
    LRCn = Common.get_LRC_num(LRC)
    header = dict([ (c, _build_occ_extraction(filename,
                                              c,
                                              cols[c]['type'],
                                              prefix,
                                              LRCn))
                    for c in cols
                    if c in set(['LRC', 'OBJ_GID']) ])
    return header


    
def get_occ_output_path(LRC, filename, cusid, tech, date= datetime.datetime.today(), CAT= _cat):
    _, dpbase = Common.get_config(cusid, tech, date, __name__)
    m = _re_occ_filename.match(filename)
    cutp, lrc = m.group(1), m.group(2)
    return dpbase.joinpath('cache/{}/occ_{}_{}.txt'.format(CAT, cutp, lrc))
