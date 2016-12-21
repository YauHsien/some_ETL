import customer
from wic import RESTRICT
from wic import etl as ETL
from wic.etl import common as Common
from wic.etl import ds as DataSource
from wic.etl.ds import ds_column as DSColumn
from wic.etl.etl_agent import ETLAgent
from wic.etl.line_extractor import LineExtractor
from wic.etl import db as ETLDB
from wic.util import file as File
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, pathlib, re



_cat = RESTRICT.CM
_re_tblname_with_ym = re.compile('^(.+)_\d\d\d\d\d\d$')



def extract_columns(cusid, tech, date):
    Common.extract_columns(cusid, tech, _cat, date, __name__)



def _get_table_name(filename, csv_flt):
    m = csv_flt.match(filename)
    tblname = m.group(1)
    return tblname

def _get_li_ex_header(header):
    header['_id'] = 'bigint'
    header['LRC'] = 'int'
    header['TIME'] = 'datetime'
    return header



def _get_line_proc(LRC, date):
    prefix = str(Common.get_prefix_by_LRC(LRC))
    LRCn = str(Common.get_LRC_num(LRC))
    TIME = '{:%Y-%m-%d}'.format(date)
    ext_cols = set(['_id', 'LRC', 'TIME'])

    def _get_line_proc(header, line, linum, prefix, LRCn):
        cols = set(line.keys())
        wanted = set(header.keys())

        line['_id'] = prefix + line['OBJ_GID']
        line['LRC'] = LRCn
        line['TIME'] = TIME

        for _, k in enumerate(cols):
            if k not in wanted:
                line.pop(k)

        return line

    return lambda header, line, linum: _get_line_proc(header, line, linum, prefix, LRCn)


    
def extract(cusid, tech, date):
    if type(date) is datetime.date:
        logger(__name__).info('extracting {} CM'.format(str(date)))

        owner_tables = Common.get_owner_tables(cusid, tech, date, _cat, __name__)
        #tblcols = DSColumn.extract_columns(cusid, tech, date, _cat, owner_tables, __name__)
        LRC_ow_ta_columns = DSColumn.get_all_columns(cusid, tech, date, _cat, __name__)
        dsconf = customer.get_default_DS_config_all(cusid, tech)

        ei = Common.extract_info(cusid, tech, date, _cat, __name__)
        for i, (LRC, zippath, filename) in enumerate(ei):
            """
            Common.perform_extraction(cusid, tech, date, _cat, LRC, zippath, filename, None, __name__)
            """
            owner = 'CMDLTE'
            tblname = _get_table_name(filename, dsconf[RESTRICT.CSV_FLT][_cat])
            tblcols = LRC_ow_ta_columns[LRC][owner, tblname]
            etl_agent = ETLAgent(tblcols, dict(), delimiter= RESTRICT.DELIMITER, newline= None, mod_name= __name__)
            outpath = Common.get_default_output_path(cusid, tech, date, _cat, filename, __name__)
            Folder.create(outpath.parent, __name__)
            ex_header = _get_li_ex_header(tblcols)
            line_proc = _get_line_proc(LRC, date)
            lineExtr = LineExtractor(ex_header, outpath, RESTRICT.DELIMITER, line_proc, __name__)
            etl_agent.add(lineExtr, 'default')
            Common.perform_extraction(cusid, tech, date, _cat, LRC, zippath, filename, etl_agent, __name__)
            etl_agent.clean()
            del etl_agent



def get_DDL_proc(owner_table_columns, date):
    if type(owner_table_columns) is dict:
        return lambda owner, tblname:\
            DSColumn._to_DDL_create(
                dict([ (tblname, owner_table_columns[owner, _tblname_without_ym(tblname)]) ]))



def get_truncate_latest_proc(date):
    return lambda owner, tblname:\
        'drop table if exists {table}_latest; create table {table}_latest like {table}_{d:%Y%m}'.format(table= _tblname_without_ym(tblname), d= date)

    
    
def _tblname_without_ym(tblnameWithYm):
    m = _re_tblname_with_ym.match(tblnameWithYm)
    if m is not None:
        return m.group(1)

    

def _format_columns(columns):
    colformat, valformat = '_id,LRC,TIME', "{_id},{LRC},'{TIME}'"
    type1 = set(['VARCHAR2', 'DATE', 'DATETIME'])
    for _, cn in enumerate(columns):
        colformat = colformat + ',{cname}'.format(cname= cn)
        if columns[cn]['type'].upper() in type1:
            valformat = valformat + ",'{{{cname}}}'".format(cname= cn)
        else:
            valformat = valformat + ',{{{cname}}}'.format(cname= cn)
    return colformat, '({})'.format(valformat)



def get_SQL_gen_proc(owner_table_columns, date):
    if type(owner_table_columns) is dict:

        c = dict([ ((owner, tblname), _format_columns(owner_table_columns[owner, tblname]))
                   for _, (owner, tblname) in enumerate(owner_table_columns) ])
        
        def _SQL_gen_proc(sqlformat, lines, owner, tblname):
            """
            ## sqlformat: 'insert into some_table ({columns}) values {values};'
            ## lines: a list of { column: value }
            """
            cs, valformat = c[owner, _tblname_without_ym(tblname)]
            vss = set()
            for _, ln in enumerate(lines):
                for _, k in enumerate(ln):
                    if ln[k] == '':
                        ln[k] = 'default'
                vs = valformat.format(**ln)
                vss.add(vs)
            vss = ','.join(vss)
            return sqlformat.format(columns = cs, values= vss)

        return lambda sqlformat, lines, owner, tblname:\
            _SQL_gen_proc(sqlformat, lines, owner, tblname)



def get_copy_latest_proc(date):
    return lambda sqlformat, lines, owner, tblname:\
        "insert into {table}_latest select * from {table}_{d:%Y%m} where TIME = '{d:%Y-%m-%d}' ".format(table= _tblname_without_ym(tblname), d= date)
    


def load(cusid, tech, date):
    _g = DSColumn.gen_all_columns(cusid, tech, date, _cat, __name__)
    c = dict([ ((owner, tblname), columns) for owner, tblname, columns in _g ])
    _DDL_proc = get_DDL_proc(c, date)
    _SQL_gen_proc = get_SQL_gen_proc(c, date)
    Common.load(_DDL_proc, _SQL_gen_proc, date, cusid, tech, _cat, __name__)



def load_latest(cusid, tech, date):
    _g = DSColumn.gen_all_columns(cusid, tech, date, _cat, __name__)
    c = dict([ ((owner, tblname), columns) for owner, tblname, columns in _g ])
    _DDL_proc = get_truncate_latest_proc(date)
    _SQL_gen_proc = get_copy_latest_proc(date)
    Common.load(_DDL_proc, _SQL_gen_proc, date, cusid, tech, _cat, __name__)
    
