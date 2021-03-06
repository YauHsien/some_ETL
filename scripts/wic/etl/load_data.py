import customer
from wic import RESTRICT
from wic.RESTRICT import (PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW, CM, FM, PM)
from wic.RESTRICT import (HOST, PORT, USER, PSWD, DB as DATABASE, CHARSET)
from wic.etl import key as SysKey
from wic import db as DB
from wic import etl as ETL
from wic.etl import common as Common
from wic.etl import ds as DataSource
from wic.etl import db as ETLDB
from wic.etl import pm as PMModule
from wic.etl.ds import ds_column as DSColumn
from wic.etl.etl_agent import ETLAgent
from wic.etl.line_extractor import LineExtractor
from wic.etl import db as ETLDB
from wic.util import file as File
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, pathlib, re, copy, time, zipfile




def lodata(cusid, tech, date, CAT, latest= True, load= False):

    def _get_owner(CAT, zipfile, zip_flt):
        if CAT == 'CM':
            return 'CMDLTE'
        if CAT == 'FM':
            return 'FM'
        m = zip_flt.match(str(zipfile.name))
        owner = m.group(1)
        return owner
    #
    def _find_DB_CAT(owner):
        if owner == 'CMDLTE':
            return 'CM'
        else:
            return owner
    #
    def _get_table_name(filename, csv_flt):
        m = csv_flt.match(filename)
        tblname = m.group(1)
        return tblname
    #
    def _get_DB_columns(cusid, tech, owner, tblname, date= None):
        tblformat = '{table}_latest' if date is None else '{table}_{d:%Y%m}'
        CAT = _find_DB_CAT(owner)
        DB_tblname = tblformat.format(table= tblname, d= date)
        dbcols = ETLDB.get_columns(cusid, tech, CAT, DB_tblname, __name__)
        return DB_tblname, dbcols
    #
    def _set_of_columns(columns):
        return set([ (c, columns[c]['column type']) for c in columns ])
    #
    def _normal_order(columns):
        keys = dict([ (columns[k]['order'], k) for k in columns ])
        return [ (keys[k], columns[keys[k]]) for k in sorted(keys) ]
    #
    def _show(tblname, columns):
        cols = _normal_order(columns)
        logger(__name__).debug(tblname)
        for c, info in cols:
            logger(__name__).debug((info['order'], c))
    #
    def _find_ID_column_name(dbconf, LRC, owner, table):
        if owner == 'CMDLTE': return SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP], 'OBJ_GID'
        if owner == 'FM':     return SysKey.system_key[RESTRICT.CATEGORY][LRC, RESTRICT.CTP], 'NE_GID'
        prefix, id_column_name, agg_terms = PMModule.get_pm_info(LRC, owner, table)
        return prefix, id_column_name
    #
    def _get_csv_columns(filepath, encoding= None, newline= None, delimiter= RESTRICT.DELIMITER):
        if File.is_path(filepath):
            with open(str(filepath), 'r', encoding= encoding, newline= newline) as fo:
                m = re.match('^(.+)$', fo.readline())
                line = m.group(1).replace('\r', '').split(delimiter)
                result = list()
                for term in line:
                    result.append('`{term}`'.format(term= term))
                return ', '.join(result)
    #
    def _find_Time_column_or_value(owner, date):
        if owner in set(['CMDLTE', 'FM']):
            return "'{d:%Y-%m-%d}'".format(d= date)
        return 'date(`PERIOD_START_TIME`)'
    #
        
    ## function chkcol(...)
    if type(date) is datetime.date:
        logger(__name__).info('loading data {date} {case}'.format(date= date, case= CAT))
        dsconf = customer.get_default_DS_config_all(cusid, tech)
        dppath = ETL.get_computed_config(cusid, tech, __name__)[RESTRICT.DATA_PATH]
        dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[CAT]
        s = [ (l, z, f) for l, z, f in Common.extract_info(cusid, tech, date, CAT, __name__) ]
        #LRCs = set([ l for _, (l, _, _) in enumerate(s) ])
        zfs = set([ (LRC, z, f,
                     _get_owner(CAT, z, dsconf[RESTRICT.ZIP_FLT][CAT]),
                     _get_table_name(f, dsconf[RESTRICT.CSV_FLT][CAT]))
                    for _, (LRC, z, f) in enumerate(s) ])
        #LRC_ow_ta_columns = DSColumn.get_all_columns(cusid, tech, date, CAT, __name__)
        #synthcols = _get_synthesized_columns(LRC_ow_ta_columns, zfs, LRCs)
        workpath = dppath.joinpath('{cusid}/{tech}/{d:%Y%m%d}/tmp/{cat}/load_data'.format(cusid= cusid, tech= tech, d= date, cat= CAT))
        Folder.create(workpath, __name__)
        tblformat = '{table}_latest' if latest else '{table}_{d:%Y%m}'

        if latest:
            for i, (o, t) in enumerate(set([ (o, t) for LRC, z, f, o, t in zfs ])):
                dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[_find_DB_CAT(o)]
                ym_latest = ETLDB.get_ym_of_latest(dbconf, t)
                if ym_latest == (date.year, date.month):
                    continue
                sql = "truncate table {table}_latest".format(table= t)
                logger(__name__).debug(sql)
                if load == True:
                    conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
                    DB.run_sql(conn, sql, prefix_tag= ' [{prefix_tag}]'.format(prefix_tag= i + 1))
                    conn.commit()
                    conn.close()
        
        zipfiles = dict()
        for i, (LRC, z, f, o, t) in enumerate(zfs):
            #_dbcols,          _cols_order    = synthcols[o, t]['columns'], synthcols[o, t]['order']
            #dbtblname_latest, dbcols1_latest = _get_DB_columns(cusid, tech, o, t)
            #dbtblname_ym,     dbcols1_ym     = _get_DB_columns(cusid, tech, o, t, date)
            #dbcols_ym,        cols_order_ym  = _get_uion_of_columns(_dbcols, dbcols1_ym)
            dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[_find_DB_CAT(o)]
            if latest:
                ym_latest = ETLDB.get_ym_of_latest(dbconf, t)
                if ym_latest == (date.year, date.month):
                    continue

            logger(__name__).info('load "{zip}/{file}"'.format(zip= str(z), file= f))
            if str(z) not in zipfiles:
                zipfiles[str(z)] = zipfile.ZipFile(str(z))
            zipfiles[str(z)].extract(f, str(workpath))
            ff = pathlib.Path(str(workpath.joinpath(f)))
            logger(__name__).debug((ff.exists(), str(ff)))

            prefix, ID_col_name = _find_ID_column_name(dbconf, LRC, o, t)
            table = tblformat.format(table= t, d= date)
            #ym_latest = ETLDB.get_ym_of_latest(dbconf, t)
            sqls = list()

            sql = "load data local infile '{local_file}' into table `{table}` fields terminated by ';' optionally enclosed by '\"' ignore 1 lines ({columns})".format(
                local_file= str(ff),
                table=      table,
                columns=    _get_csv_columns(ff)
            )
            sqls.append(sql)

            
            if t.upper() in set(['PCOFNS_PS_SCTP_ENB_RAW']):
                sql = "update `{table}` set `_id` = concat('{prefix}', {id_column}), `LRC` = {LRCn}, `TIME` = {TIMEcov} where date(`PERIOD_START_TIME`) = '{d:%Y-%m-%d}' and `_id` is null".format(
                    table=     table,
                    prefix=    prefix,
                    id_column= ID_col_name,
                    LRCn=      Common.get_LRC_num(LRC),
                    TIMEcov=   _find_Time_column_or_value(o, date),
                    d=         date
                )
            else:
                sql = "update `{table}` set `_id` = concat('{prefix}', {id_column}), `LRC` = {LRCn}, `TIME` = {TIMEcov} where `_id` is null".format(
                    table=     table,
                    prefix=    prefix,
                    id_column= ID_col_name,
                    LRCn=      Common.get_LRC_num(LRC),
                    TIMEcov=   _find_Time_column_or_value(o, date)
                )
            sqls.append(sql)

            if len(sqls) > 0:
                sql = ';\n'.join(['start transaction'] + sqls + ['commit'])
                logger(__name__).debug(sql)
                with open(str(workpath.joinpath('{table}.sql'.format(table= t))), 'w') as fo:
                    fo.write(sql)
                if load == True:
                    conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
                    DB.run_sql(conn, sql, prefix_tag= ' [{prefix_tag}]'.format(prefix_tag= i + 1))
                    conn.commit()
                    conn.close()
                    
            #File.remove(ff, __name__)

        for zf in zipfiles:
            zipfiles[zf].close()
        del zipfiles
        #Folder.remove(workpath, __name__)

        if latest:
            for i, (o, t) in enumerate(set([ (o, t) for LRC, z, f, o, t in zfs ])):
                dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[_find_DB_CAT(o)]
                ym_latest = ETLDB.get_ym_of_latest(dbconf, t)
                if ym_latest == (date.year, date.month):
                    continue
                
                table_latest = '{table}_latest'.format(table= t)
                table_ym =     '{table}_{d:%Y%m}'.format(table= t, d= date)

                sql = 'insert into `{table_ym}` select * from `{table_latest}`'.format(
                    table_ym=     table_ym,
                    table_latest= table_latest
                )

                logger(__name__).debug(sql)
                if load == True:
                    conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
                    DB.run_sql(conn, sql, prefix_tag= ' [{prefix_tag}]'.format(prefix_tag= i + 1))
                    conn.commit()
                    conn.close()
#   #   #   #   #   #
