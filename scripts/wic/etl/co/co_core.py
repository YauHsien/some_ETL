from wic import RESTRICT
from wic.etl import common as Common
from wic.etl import column
from wic.etl import db as ETLDB
from wic.etl.ds import ds_column as DSColumn
from wic.etl import extraction
from wic.etl.etl_agent import ETLAgent
from wic.etl.line_extractor import LineExtractor
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, pathlib, json



_cat = RESTRICT.CO



def download_columns(cusid, tech):
    colfpath = column.find_column_file_path(cusid, tech, _cat)
    cols = ETLDB.get_columns(cusid, tech, _cat, mod_name= __name__)
    File.dump_JSON(colfpath, cols, __name__)



## ETL-agent header
def get_etl_agent_header(cusid, tech):
    colfpath = column.find_file_path(cusid, tech, _cat)
    cols = ETLDB.get_columns(cusid, tech, _cat, mod_name= __name__)
    return dict([ (c, cols[c]['type']) for c in cols ])



"""
# _id:                extraction.prepend_category('CO_GID')
# _parent_id:         extraction.prepend_category('CO_PARENT_GID')
# _level:             extraction.apply(f, 'CO_DN')
# LRC:                extraction.const(LRC_Num)
# CO_GID:             extraction.extract('CO_GID')
# CO_OC_ID
# CO_SYS_VERSION or CO_OCV_SYS_VERSION
# CO_OBJECT_INSTANCE
# CO_PARENT_GID
# CO_DN
# CO_NAME
"""
def _build_extraction(filename, column, col_type, prefix, LRCn):

    if column == '_id':
        return lambda obj, obj1, line, linum:\
            extraction.cast(
                obj1,
                filename,
                linum,
                column,
                '{}{}'.format(prefix, Common.lnval(line, 'CO_GID')),
                col_type,
                __name__)

    elif column == '_parent_id':
        return lambda obj, obj1, line, linum:\
            extraction.cast(
                obj1,
                filename,
                linum,
                column,
                '{}'.format(prefix) if Common.lnval(line, RESTRICT.CO_PARENT_GID) is None else '{}{}'.format(prefix, Common.lnval(line, RESTRICT.CO_PARENT_GID)),
                col_type,
                __name__)

    elif column == '_level':
        return lambda obj, obj1, line, linum:\
            extraction.cast(
                obj1,
                filename,
                linum,
                column,
                (lambda x: pathlib.Path(x).stem.split('-')[0])(Common.lnval(line, 'CO_DN')),
                col_type,
                __name__)

    elif column in set(['LRC']):
        return lambda obj, obj1, line, linum: LRCn

    elif column == RESTRICT.CO_PARENT_GID:
        return lambda obj, obj1, line, linum: str() if Common.lnval(line, RESTRICT.CO_PARENT_GID) is None else Common.lnval(line, RESTRICT.CO_PARENT_GID)
    
    elif column in set(['CO_GID', 'CO_OC_ID', 'CO_SYS_VERSION',
                        'CO_OBJECT_INSTANCE',
                        'CO_DN', 'CO_NAME']):
        return lambda obj, obj1, line, linum:\
            extraction.cast(
                obj1,
                filename,
                linum,
                column,
                Common.lnval(line, column),
                col_type,
                __name__)



## line-extractor header
def get_li_ex_header(cusid, tech, LRC, filename):
    colfpath = column.find_file_path(cusid, tech, _cat)
    cols = ETLDB.get_columns(cusid, tech, _cat, mod_name= __name__)
    prefix = Common.get_prefix_by_LRC(LRC)
    LRCn = Common.get_LRC_num(LRC)
    header = dict([ (c, _build_extraction(filename,
                                          c,
                                          cols[c]['type'],
                                          prefix,
                                          LRCn))
                    for c in cols ])
    for c in [ x for x in ['__id', '_type', '_additional1', '_additional2', 'create_date', 'last_update'] if x in header ]:
        header.pop(c)
    return header



def get_line_proc(LRC):
    prefix = str(Common.get_prefix_by_LRC(LRC))
    LRCn = str(Common.get_LRC_num(LRC))
    ext_cols = set(['_id', '_parent_id', '_level', 'LRC'])

    def _get_line_proc(header, line, linum, prefix, LRCn):
        cols = set(line.keys())
        wanted = set(header.keys())
        
        CO_DN = line['CO_DN']
        _level = CO_DN[ CO_DN.rfind('/') + 1 : CO_DN.rfind('-') ]
        
        line['_id'] = prefix + line['CO_GID']
        line['_parent_id'] = prefix + line['CO_PARENT_GID']
        line['_level'] = _level
        line['LRC'] = LRCn

        if 'CO_SYS_VERSION' not in cols:
            line['CO_SYS_VERSION'] = line['CO_OCV_SYS_VERSION']

        for _, k in enumerate(cols):
            if k not in wanted:
                line.pop(k)
        
        return line
    
    return lambda header, line, linum: _get_line_proc(header, line, linum, prefix, LRCn)



def extract(cusid, tech, date):
    if type(date) is datetime.date:
        logger(__name__).info('extracting {} CO'.format(str(date)))
        #ymd = '{:%Y%m%d}'.format(date)
        ei = Common.extract_info(cusid, tech, date, _cat, __name__)
        for i, (LRC, zippath, filename) in enumerate(ei):
            """
            Common.perform_extraction(cusid, tech, date, _cat, LRC, zippath, filename, None, __name__)
            """
            etl_agent = ETLAgent(get_etl_agent_header(cusid, tech),
                                 { 'CO_OCV_SYS_VERSION': 'CO_SYS_VERSION' },
                                 RESTRICT.DELIMITER,
                                 __name__)
            outpath = Common.get_default_output_path(cusid, tech, date, _cat, filename, __name__)
            Folder.create(outpath.parent, __name__)
            etl_agent.add(adapter= LineExtractor(get_li_ex_header(cusid, tech, LRC, filename),
                                                 outpath,
                                                 RESTRICT.DELIMITER,
                                                 get_line_proc(LRC),
                                                 __name__),
                          name= 'default')
            Common.perform_extraction(cusid, tech, date, _cat, LRC, zippath, filename, etl_agent, __name__)
            etl_agent.clean()
            del etl_agent



def get_DDL_proc(cusid, tech, tblname, columns):
    return lambda owner, tblname1: DSColumn._to_DDL_object_control(tblname, columns)



def get_SQL_gen_proc(cusid, tech, tblname, columns):

    valformat = '({_id}, {_parent_id}, {_level}, {LRC}, {CO_GID}, {CO_OC_ID}, {CO_SYS_VERSION}, {CO_OBJECT_INSTANCE}, {CO_PARENT_GID}, {CO_DN}, {CO_NAME})'
    sqlformat = """
INSERT INTO xinos_amb_config.object_control
(`_id`, `_parent_id`, `_level`, LRC, CO_GID, CO_OC_ID, CO_SYS_VERSION, CO_OBJECT_INSTANCE, CO_PARENT_GID, CO_DN, CO_NAME)
VALUES
{values}
                """
    type1 = set(['VARCHAR', 'DATE', 'DATETIME', 'TIMESTAMP(6)'])

    def _SQL_gen_proc(sqlformat, lines, owner, tblname):
        vss = set()
        for _, ln in enumerate(lines):
            for _, k in enumerate(ln):
                if ln[k] == '':
                    ln[k] = 'default'
                elif k in columns and columns[k]['type'].upper() in type1:
                    ln[k] = "'{}'".format(ln[k])
                #logger(__name__).debug(ln[k])
            vs = valformat.format(**ln)
            #logger(__name__).debug(valformat)
            #logger(__name__).debug(vs)
            vss.add(vs)
        vss = ','.join(vss)
        return sqlformat.format(values= vss)
    
    return lambda sqlformat1, lines, owner, tblname:\
        _SQL_gen_proc(sqlformat, lines, owner, tblname)



"""
'CO_OBJECT_INSTANCE': {'len': 255, 'type': 'varchar'},
'CO_NAME': {'len': 255, 'type': 'varchar'},
'_parent_id': {'len': None, 'type': 'bigint'},
'CO_DN': {'len': 255, 'type': 'varchar'},
'_id': {'len': None, 'type': 'bigint'},
'CO_PARENT_GID': {'len': None, 'type': 'int'},
'CO_OC_ID': {'len': None, 'type': 'int'},
'CO_SYS_VERSION': {'len': 255, 'type': 'varchar'},
'CO_GID': {'len': None, 'type': 'int'},
'LRC': {'len': None, 'type': 'tinyint'},
"""
def _get_columns(cusid, tech):
    dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[_cat]
    tblname = dbconf[RESTRICT.TABLE]
    columns = ETLDB.get_columns(cusid, tech, _cat, tblname= tblname, mod_name= __name__)
    for c in ['__id', '_type', '_additional1', '_additional2', 'create_date', 'last_update']:
        columns.pop(c)
    for c in columns:
        print(c, columns[c])
    return tblname, columns


def load(cusid, tech, date):
    #_g = DSColumn.gen_all_columns(cusid, tech, date, _cat, __name__)
    #c = dict([ ((owner, tblname), columns) for owner, tblname, columns in _g ])
    tblname, columns = _get_columns(cusid, tech)
    _DDL_proc = get_DDL_proc(cusid, tech, tblname, columns)
    _SQL_gen_proc = get_SQL_gen_proc(cusid, tech, tblname, columns)
    Common.load(_DDL_proc, _SQL_gen_proc, date, cusid, tech, _cat, __name__)
