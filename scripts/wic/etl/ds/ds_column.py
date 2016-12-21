import wic
from wic import RESTRICT
from wic import etl as ETL
from wic.etl import common as Common
from wic.etl import ds as DataSource
from wic.etl.ds import ds_DB_COLUMNS as DS_DBColumns
from wic.util import file as File
from wic.util.logging import logger
import sys, datetime, zipfile, re



def extract_info(cusid, tech, date, mod_name):
    if type(date) is datetime.date:
        cfgbase, dpbase = Common.get_config(cusid, tech, date, mod_name)
        LRC_z_s = Common.download_zips(cusid, tech, RESTRICT.DB, dpbase, cfgbase, limit= None, mod_name= mod_name)
        reCAT = DataSource.get_computed_config(cusid, tech, __name__)[RESTRICT.CSV_FLT][RESTRICT.DB]
        for i, (LRC, zippath) in enumerate(LRC_z_s):
            zf = zipfile.ZipFile(str(zippath))
            infolist = zf.infolist()
            zf.close()
            for _, f in enumerate([ zi.filename for _, zi in enumerate(infolist)
                                    if reCAT.match(zi.filename) ]):
                yield (LRC, zippath, f)



def _get_regex(CAT):
    if RESTRICT.CM == CAT:
        return re.compile('^CMDLTE;(.|\n)+$')
    elif RESTRICT.FM == CAT:
        return re.compile('^FM;FX_ALARM;(.|\n)+$')
    elif RESTRICT.PM == CAT:
        return re.compile('^(PCOFNSRAW|PCOFNGRAW|IMSCSFRAW|IMSHSSRAW|MADNHRRAW|MADODCRAW|IMSDRARAW|XMLNSSRAW|NOKOBWRAW|NOKOMWRAW|NOKIUMRAW);(.|\n)+$')
    elif CAT in set([RESTRICT.PCOFNSRAW, RESTRICT.PCOFNGRAW, RESTRICT.IMSCSFRAW,
                     RESTRICT.IMSHSSRAW, RESTRICT.MADNHRRAW, RESTRICT.MADODCRAW, RESTRICT.IMSDRARAW,
                     RESTRICT.XMLNSSRAW, RESTRICT.NOKOBWRAW, RESTRICT.NOKOMWRAW,
                     RESTRICT.NOKIUMRAW]):
        return re.compile('^({term});(.|\n)+$'.format(term= CAT))



def _get_columns(zippath, filename, tmppath, CAT):

    if File.is_path(tmppath):
        
        zf = zipfile.ZipFile(str(zippath))
        zf.extract(filename, str(tmppath))
        zf.close()

        reCAT = _get_regex(CAT)

        result = dict()
        with open(str(tmppath.joinpath(filename)), 'r', newline= '\r\n', encoding= 'latin-1') as fo:

            header = dict()
            for _, ln in enumerate(fo):
                line = ln.rstrip().split(RESTRICT.DELIMITER)
                if len(line) > 0:
                    for i, h in enumerate(line):
                        header[str(h.encode(), sys.getdefaultencoding())] = i
                    break

            for _, ln in enumerate([ x for x in iter(fo) if reCAT.match(x) ]):

                line = ln.rstrip().split(RESTRICT.DELIMITER)
                
                key_tbl = ( str(line[header['OWNER']].encode(), sys.getdefaultencoding()),
                            str(line[header['TABLE_NAME']].encode(), sys.getdefaultencoding()) )
                if key_tbl not in result:
                    result[key_tbl] = dict()
                columns = result[key_tbl]

                key_col = str(line[header['COLUMN_NAME']].encode(), sys.getdefaultencoding())
                if key_col not in columns:
                    columns[key_col] = dict([ ('type',
                                               str(line[header['DATA_TYPE']].encode(),
                                                   sys.getdefaultencoding())),
                                              ('len',
                                               str(line[header['DATA_LENGTH']].encode(),
                                                   sys.getdefaultencoding())),
                                              ('precision',
                                               str(line[header['DATA_PRECISION']].encode(),
                                                   sys.getdefaultencoding())),
                                              ('scale',
                                               str(line[header['DATA_SCALE']].encode(),
                                                   sys.getdefaultencoding()))  ])

        tmppath.joinpath(filename).unlink()
        return result

"""
## get LRC-owner-table-columns dictionary:
## LRC => (owner, table name) => column name => column information
"""
def get_all_columns(cusid, tech, date, CAT, mod_name):
    if type(date) is datetime.date:
        dppath = ETL.get_computed_config(cusid, tech, __name__)[RESTRICT.DATA_PATH]
        tmppath = dppath.joinpath('{}/{}/{:%Y%m%d}/tmp'.format(cusid, tech, date))
        LRC_ow_ta_columns = dict()
        for LRC, z1, f1 in extract_info(cusid, tech, date, mod_name):
            LRC_ow_ta_columns[LRC] = DS_DBColumns.get_ow_ta_columns(z1, f1, tmppath, CAT)
        return LRC_ow_ta_columns

def gen_all_columns(cusid, tech, date, CAT, mod_name):
    if type(date) is datetime.date:

        dppath = ETL.get_computed_config(cusid, tech, __name__)[RESTRICT.DATA_PATH]
        tmppath = dppath.joinpath('{}/{}/{:%Y%m%d}/tmp'.format(cusid, tech, date))
        for _, z1, f1 in extract_info(cusid, tech, date, mod_name):

            columns = _get_columns(z1, f1, tmppath, CAT)
            for owner, tblname in columns:
                yield owner, tblname, columns[owner, tblname]

     

"""
## table name => column name => column information
"""
def extract_columns(cusid, tech, date, CAT, tables, mod_name):
    if type(date) is datetime.date and type(tables) is set:

        e = enumerate([ (owner, tblname, columns)
                        for owner, tblname, columns in gen_all_columns(cusid, tech, date, CAT, mod_name)
                        if (owner, tblname) in tables ])

        tbl_columns = dict()
        for _, (ow, tn, cs) in e:

            if tn in tbl_columns:
                for c in [ col for col in cs if col not in tbl_columns[tn] ]:
                    tbl_columns[tn][c] = cs[c]
                        
            else:
                tbl_columns[tn] = dict()
                for col in cs:
                    tbl_columns[tn][col] = cs[col]

        return tbl_columns



def to_sql(create= None, tblname= None, add_column= None, change_column= None):

    result = list()
    
    if create is not None:
        result.append(_to_DDL_create(create))
        
    if add_column is not None:
        result.append(_to_DDL_add_column(tblname, add_column))
        
    if change_column is not None:
        result.append(_to_DDL_change_column(tblname, change_column))
        
    return ';\n'.join([ x for x in filter(lambda x: x is not None, result) ])



def _try_int(value):
    try:
        return int(value)
    except:
        return None



def type_equal(desc1, desc2):
    if type(desc1) is dict and type(desc2) is dict:
        return (desc1['type'].upper() == 'VARCHAR2' and desc2['type'].lower() == 'varchar' and int(desc1['len']) == desc2['len'])\
            or (desc1['type'].upper() == 'DATE' and desc2['type'].lower() == 'datetime')\
            or (desc1['type'].upper() == 'DATETIME' and desc2['type'].lower() == 'datetime')\
            or (desc1['type'].upper() == 'TIMESTAMP(6)' and desc2['type'].lower() == 'timestamp')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'decimal')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'numeric')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'tinyint')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'smallint')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'mediumint')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'int')\
            or (desc1['type'].upper() == 'NUMBER' and desc2['type'].lower() == 'bigint')
    else:
        return False


## return column type in string representation
def ora_to_my_column_type(colname, tlps):
    if type(tlps) is dict:
        if tlps['type'].upper() == 'VARCHAR2':
            if int(tlps['len']) > 2048:
                return 'VARCHAR(1024)'
            else:
                return 'VARCHAR({length})'.format(length= tlps['len'])
        elif tlps['type'].upper() in set(['DATE', 'DATETIME']):
            return 'DATETIME'
        elif tlps['type'].upper() == 'TIMESTAMP(6)':
            return 'TIMESTAMP'
        elif tlps['type'].upper() == 'NUMBER':
            p = tlps['precision']
            s = tlps['scale']
            if p is None and s is None:
                return 'DECIMAL({precision},0)'.format(precision= tlps['len'])
            elif p is not None and (s == 0 or s is None):
                if p <= 3:    return 'TINYINT(4)'
                elif p <= 5:  return 'SMALLINT(6)'
                elif p <= 7:  return 'MEDIUMINT(9)'
                elif p <= 10: return 'INT(11)'
                elif p <= 19: return 'BIGINT(20)'
            else:
                return 'DECIMAL({precision},{scale})'.format(precision= p, scale= s)
            


"""
# tlps: type-len-precision-scale
"""
def _conclude_type(tlps):

    cat_str = set(['VARCHAR2', 'VARCHAR'])
    cat_dt = set(['DATE', 'DATETIME'])
    cat_int = set(['BIGINT', 'INT', 'TINYINT', 'SMALLINT'])
    
    if type(tlps) is dict:

        if tlps['type'].upper() in cat_str:
            if int(tlps['len']) > 2048:
                return "VARCHAR(1024) default ''"
            else:                
                return "{}({}) default ''".format('VARCHAR', tlps['len'])

        elif tlps['type'].upper() in cat_dt:
            return "DATETIME"

        elif tlps['type'].upper() == 'TIMESTAMP(6)':
            return 'TIMESTAMP'

        elif tlps['type'].upper() in cat_int:
            return tlps['type']

        elif tlps['type'].upper() == 'NUMBER':

            p = _try_int(tlps['precision'])
            s = _try_int(tlps['scale'])
            desc = '{type} default null'
            
            if p is None and s is None:
                t = 'NUMERIC({precision},0)'.format(precision= tlps['len'])
            elif p is not None and (s == 0 or s is None):
                if p <= 3:    t = 'TINYINT(4)'
                elif p <= 5:  t = 'SMALLINT(6)'
                elif p <= 7:  t = 'MEDIUMINT(9)'
                elif p <= 10: t = 'INT(11)'
                elif p <= 19: t = 'BIGINT(20)'
            else:
                t = 'NUMERIC({precision},{scale})'.format(precision= p, scale= s)
            
            return desc.format(type= t)
                    


def _to_DDL_create(create):
    if type(create) is dict:

        sqls = list()
        for tblname in create:
            cols = list()
            cols.append('_id bigint')
            cols.append('LRC int')
            cols.append('TIME datetime')

            for cname in create[tblname]:
                typedesc = _conclude_type(create[tblname][cname])
                col = '{column} {type}'.format(
                    column= cname,
                    type= typedesc
                )
                cols.append(col)
                
            col_list = ',\n    '.join(cols)
            sqls.append('create table if not exists {} (\n    {},\n    KEY `LRC__id` (`LRC`, `_id`),\n    KEY `TIME` (`TIME`),\n    KEY `_id` (`_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8'.format(tblname, col_list))
                               
        return ';\n'.join(sqls)



def _to_DDL_object_control(tblname, columns):
    if tblname == 'object_control':
        ddlformat = """
CREATE TABLE if not exists `object_control` (
  {},
  KEY `_id` (`_id`),
  KEY `LRC` (`LRC`),
  KEY `CO_GID` (`CO_GID`),
  KEY `CO_PARENT_GID` (`CO_PARENT_GID`),
  KEY `CO_OBJECT_INSTANCE` (`CO_OBJECT_INSTANCE`),
  KEY `CO_NAME` (`CO_NAME`),
  KEY `_level` (`_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Object Control'
                    """
        cols = list()
        for cname in columns:
            typedesc = _conclude_type(columns[cname])
            col = '{column} {type}'.format(
                column= cname,
                type= typedesc
            )
            cols.append(col)
        col_list = ',\n    '.join(cols)
        return ddlformat.format(col_list)



def _to_DDL_add_column(tblname, add_column):
    if type(add_column) is dict:

        cols = list()
        for cname in add_column:
            col = 'add column {} {} DEFAULT NULL'.format(cname, _conclude_type(add_column[cname]))
            cols.append(col)

        col_list = ',\n    '.join(cols)

        return 'alter table {}\n    {}'.format(tblname, col_list)



def _to_DDL_change_column(tblname, change_column):
    if type(change_column) is dict:

        cols = list()
        for cname in change_column:
            col = 'change column {0} {0} {1} DEFAULT NULL'.format(cname, _conclude_type(change_column[cname]))
            cols.append(col)

        col_list = ',\n    '.join(cols)

        return 'alter table {}\n    {}'.format(tblname, col_list)
