import customer
from wic import RESTRICT
from wic.RESTRICT import (PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW, CM, FM, PM)
from wic.RESTRICT import (HOST, PORT, USER, PSWD, DB as DATABASE, CHARSET)
from wic import db as DB
from wic import etl as ETL
from wic.etl import common as Common
from wic.etl import ds as DataSource
from wic.etl import db as ETLDB
from wic.etl.ds import ds_column as DSColumn
from wic.etl.etl_agent import ETLAgent
from wic.etl.line_extractor import LineExtractor
from wic.etl import db as ETLDB
from wic.util import file as File
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, pathlib, re, copy, time




def chkcol(cusid, tech, date, CAT, latest= True, load= False):

    def _get_owner(CAT, zipfile, zip_flt):
        if CAT == 'CM':
            return 'CMDLTE'
        if CAT == 'FM':
            return 'FM'
        m = zip_flt.match(str(zipfile.name))
        owner = m.group(1)
        return owner

    def _find_DB_CAT(owner):
        if owner == 'CMDLTE':
            return 'CM'
        else:
            return owner
    
    def _get_table_name(filename, csv_flt):
        m = csv_flt.match(filename)
        tblname = m.group(1)
        return tblname

    #def _get_DB_table_name(filename, csv_flt, date= None):
    #    m = csv_flt.match(filename)
    #    tblname = m.group(1)
    #    if date is None:
    #        tblformat = '{table}_latest'
    #    else:
    #        tblformat = '{table}_{d:%Y%m}'
    #    return tblformat.format(table= tblname, d= date)

    def _ora_to_my_db_columns(allcols, cols_order):
        result = dict()
        for i in cols_order:
            colname = cols_order[i]
            result[colname] = dict()
            col = result[colname]
            col['order'] = i
            col['column type'] = DSColumn.ora_to_my_column_type(colname, allcols[colname]).lower()
        return result
    
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
    def _get_uion_of_columns(columns1, columns2):
        """ fake columns
        #columns1.pop('LRC')
        #for c in columns1:
        #    if c != '_id':
        #        columns1[c]['order'] = columns1[c]['order'] - 1
        #columns1['hello'] = dict([ ('order', len(columns1) + 1) ])
        #columns2.pop('TIME')
        #_show('data source', columns1)
        #_show('ym table', columns2)
        """
        cols_order = dict()
        for i, (c, info) in enumerate(list(columns1.items()) + list(columns2.items())):
            j = info['order']
            if j not in cols_order:
                cols_order[j] = c
            elif type(cols_order[j]) is list:
                cols_order[j].append(c)
            elif cols_order[j] != c:
                cols_order[j] = list([ cols_order[j], c ])
        cols_order1 = dict()
        k = 1
        columns = set()
        for i, j in enumerate(sorted(cols_order)):
            if type(cols_order[j]) is list:
                for t in cols_order[j]:
                    if t not in columns:
                        cols_order1[k] = t
                        k = k + 1
                        columns.add(t)
            else:
                t = cols_order[j]
                if t not in columns:
                    cols_order1[k] = t
                    k = k + 1
                    columns.add(t)
        del columns
        del cols_order
        columns3 = dict()
        for i in cols_order1:
            c = cols_order1[i]
            if c in columns1:
                columns3[c] = copy.copy(columns1[c])
            elif c in columns2:
                columns3[c] = copy.copy(columns2[c])
            columns3[c]['order'] = i
        """ test union of columns
        #vote = True
        #for c in columns3:
        #    if c != cols_order1[columns3[c]['order']]:
        #        vote = False
        #if not vote:
        #    logger(__name__).debug(cols_order1)
        #    logger(__name__).debug(columns3)
        #    for c in columns3:
        #        logger(__name__).debug(c == cols_order1[columns3[c]['order']])
        """
        return columns3, cols_order1
        
    #
        
    ## function chkcol(...)
    if type(date) is datetime.date:
        logger(__name__).info('checking column {date} {case}'.format(date= date, case= CAT))
        dsconf = customer.get_default_DS_config_all(cusid, tech)
        dppath = ETL.get_computed_config(cusid, tech, __name__)[RESTRICT.DATA_PATH]
        dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[CAT]
        s = [ (l, z, f) for l, z, f in Common.extract_info(cusid, tech, date, CAT, __name__) ]
        LRCs = set([ l for _, (l, _, _) in enumerate(s) ])
        zfs = set([ (z, f,
                     _get_owner(CAT, z, dsconf[RESTRICT.ZIP_FLT][CAT]),
                     _get_table_name(f, dsconf[RESTRICT.CSV_FLT][CAT]))
                    for _, (_, z, f) in enumerate(s) ])
        LRC_ow_ta_columns = DSColumn.get_all_columns(cusid, tech, date, CAT, __name__)
        synthcols = _get_synthesized_columns(LRC_ow_ta_columns, zfs, LRCs)
        workpath = dppath.joinpath('{cusid}/{tech}/{d:%Y%m%d}/tmp/{cat}/chkcol'.format(cusid= cusid, tech= tech, d= date, cat= CAT))
        Folder.create(workpath, __name__)

        for o, t in set([ (o, t) for z, f, o, t in zfs ]):
            if o == 'CMDLTE' and t == 'C_LTE_LNCEL':
                continue
            
            _dbcols,          _cols_order    = synthcols[o, t]['columns'], synthcols[o, t]['order']
            dbtblname_latest, dbcols1_latest = _get_DB_columns(cusid, tech, o, t)
            dbtblname_ym,     dbcols1_ym     = _get_DB_columns(cusid, tech, o, t, date)
            dbcols_ym,        cols_order_ym  = _get_uion_of_columns(_dbcols, dbcols1_ym)

            dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[_find_DB_CAT(o)]
            ym_latest = ETLDB.get_ym_of_latest(dbconf, t)
            ddls = list()

            if dbcols1_ym == dict():
                ddls.append( _build_DDL_my_create(dbconf[RESTRICT.DB], dbtblname_ym, dbcols_ym, cols_order_ym) )
                if latest:
                    ddls.append( _build_DDL_my_drop(dbconf[RESTRICT.DB], dbtblname_latest) )
                    ddls.append( _build_DDL_my_create(dbconf[RESTRICT.DB], dbtblname_latest, dbcols_ym, cols_order_ym) )
                elif ym_latest == (date.year, date.month):
                    dbcols_latest, cols_order_latest = _get_uion_of_columns(_dbcols, dbcols1_latest)
                    if len(dbcols_latest) != len(dbcols1_latest) or _set_of_columns(dbcols_latest) != _set_of_columns(dbcols1_latest):
                        ddls.append( _build_DDL_my_alter(dbconf[RESTRICT.DB], dbtblname_latest, dbcols_latest, cols_order_latest, dbcols1_latest) )
                    
            elif len(dbcols_ym) != len(dbcols1_ym) or _set_of_columns(dbcols_ym) != _set_of_columns(dbcols1_ym):
                logger(__name__).debug(len(dbcols_ym) != len(dbcols1_ym))
                logger(__name__).debug(_set_of_columns(dbcols_ym) != _set_of_columns(dbcols1_ym))
                ddls.append( _build_DDL_my_alter(dbconf[RESTRICT.DB], dbtblname_ym, dbcols_ym, cols_order_ym, dbcols1_ym) )
                if latest:
                    ddls.append( _build_DDL_my_drop(dbconf[RESTRICT.DB], dbtblname_latest) )
                    ddls.append( _build_DDL_my_create(dbconf[RESTRICT.DB], dbtblname_latest, dbcols_ym, cols_order_ym) )
                elif ym_latest == (date.year, date.month):
                    dbcols_latest, cols_order_latest = _get_uion_of_columns(_dbcols, dbcols1_latest)
                    if len(dbcols_latest) != len(dbcols1_latest) or _set_of_columns(dbcols_latest) != _set_of_columns(dbcols1_latest):
                        ddls.append( _build_DDL_my_alter(dbconf[RESTRICT.DB], dbtblname_latest, dbcols_latest, cols_order_latest, dbcols1_latest) )

            if len(ddls) > 0:
                ddl = ';\n'.join(['start transaction'] + ddls + ['commit'])
                logger(__name__).debug(ddl)
                with open(str(workpath.joinpath('{table}.sql'.format(table= t))), 'w') as fo:
                    fo.write(ddl)
                if load == True:
                    conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
                    DB.run_sql(conn, ddl)
                    conn.close()
    #

def get_synthesized_columns(cusid, tech, date, CAT, mod_name= __name__):
    s = [ (l, z, f) for l, z, f in Common.extract_info(cusid, tech, date, CAT, __name__) ]
    LRCs = set([ l for _, (l, _, _) in enumerate(s) ])
    zfs = set([ (z, f,
                 _get_owner(CAT, z, dsconf[RESTRICT.ZIP_FLT][CAT]),
                 _get_table_name(f, dsconf[RESTRICT.CSV_FLT][CAT]))
                for _, (_, z, f) in enumerate(s) ])
    LRC_ow_ta_columns = DSColumn.get_all_columns(cusid, tech, date, CAT, __name__)
    synthcols = _get_synthesized_columns(LRC_ow_ta_columns, zfs, LRCs)
    return synthcols

"""
## get syntesized columns from data source
## (owner, table name) => (allcols, cols_order)
## - allcols: column name => column description
## - cols_order: int => column name
"""
def _get_synthesized_columns(LRC_ow_ta_columns, zfs, LRCs):
    result = dict()
    for z, f, owner, tblname in zfs:
        allcols = dict([
            ('_id',  dict([ ('column type', 'bigint(20)'), ('order', 1) ])),
            ('LRC',  dict([ ('column type', 'int(11)'), ('order', 2) ])),
            ('TIME', dict([ ('column type', 'datetime'), ('order', 3) ]))
        ])
        cols_order = dict([ (1, '_id'), (2, 'LRC'), (3, 'TIME') ])
        for LRC in LRCs:
            if (owner, tblname) in LRC_ow_ta_columns[LRC]:
                columns = LRC_ow_ta_columns[LRC][owner, tblname]
                for c in columns:
                    if c not in allcols:
                        allcols[c] = copy.copy(columns[c])
                        allcols[c]['column type'] = DSColumn.ora_to_my_column_type(c, columns[c]).lower()
                        for t in set(['precision', 'scale', 'type', 'len']):
                            allcols[c].pop(t)
                        cols_order[c] = list([ columns[c]['order'] ])
                    else:
                        cols_order[c].append( columns[c]['order'] )
        """
        ## raw structure
        #logger(__name__).debug((owner, tblname))
        #logger(__name__).debug(allcols)
        #logger(__name__).debug(cols_order)
        """
        for c in set([ k for k in allcols.keys() if k not in set(['_id', 'LRC', 'TIME']) ]):
            ## find synthesis order
            new_order = max(cols_order[c]) + 3
            if new_order in cols_order:
                co = cols_order[new_order]
                if type(co) is list:
                    co.append(c)
                elif co != c:
                    cols_order[new_order] = list([co, c])
            else:
                cols_order[new_order] = c
            cols_order.pop(c)
        """
        #if tblname == 'MADNHR_PS_DIA_DIA_RAW':
        #    logger(__name__).debug(cols_order)
        """
        cols_order1 = dict()
        i = 1
        for j in cols_order:
            ## adjust order of each column
            k = cols_order[j]
            if type(k) is list:
                for term in k:
                    cols_order1[i] = term
                    i = i + 1
            else:
                cols_order1[i] = k
                i = i + 1
                if k not in set(['_id', 'LRC', 'TIME']):
                    allcols[k]['order'] = allcols[k]['order'] + 3
        """
        ## refined structure
        #logger(__name__).debug((owner, tblname))
        #logger(__name__).debug(allcols)
        #if tblname == 'MADNHR_PS_DIA_DIA_RAW':
        #    logger(__name__).debug(cols_order1)
        """
        result[owner, tblname] = dict([ ('columns', allcols), ('order', cols_order1) ])
    return result

def _build_column_desc(ctype):
    rt = re.compile('^(.+)[(](.+)$')
    m = rt.match(ctype)
    if m is None:
        if ctype in set(['datetime', 'date', 'time', 'float', 'double', 'int', 'tinyint', 'smallint', 'mediumint']):
            return 'default null'
        else:
            return ''
    elif m.group(1) in set(['decimal']):
        return 'default null'
    elif m.group(1) in set(['varchar']):
        return "default ''"
    elif m.group(1) in set(['decimal', 'bigint', 'int', 'tinyint', 'smallint', 'mediumint']):
        return 'default null'
    else:
        return ''

def _build_DDL_my_create(database, tblname, dbcols, cols_order):

    ddlformat = """
create table
if not exists
`{database}`.`{table}` (
    {columns},
    KEY `LRC__id` (`LRC`, `_id`),
    KEY `TIME` (`TIME`),
    KEY `_id` (`_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
    """.strip()
    columns = list()
    for i in sorted(cols_order):
        c = cols_order[i]
        columns.append(
            '`{cname}` {ctype} {desc}'.format(
                cname= c,
                ctype= dbcols[c]['column type'],
                desc=  _build_column_desc(dbcols[c]['column type'])
            ))
    columns = ',\n    '.join(columns)
    ddl = ddlformat.format(database= database, table= tblname, columns= columns)
    return ddl

def _build_DDL_my_drop(database, tblname):
    return 'drop table if exists `{database}`.`{table}`'.format(database= database, table= tblname)
    
def _build_DDL_my_alter(database, tblname, dbcols, cols_order, dbcols_old):

    def _find_prev_column(cols_order, i, dbcols_old):
        i1 = i
        while True:
            i1 = i1 - 1
            c = cols_order[i1]
            if c in dbcols_old:
                return c
    #
    
    states = dict()
    for c in dbcols:
        if c not in states:
            states[c] = list([ dbcols[c]['order'] ])
        else:
            states[c].append(dbcols[c]['order'])
    for c in dbcols_old:
        if c not in states:
            states[c] = list([ dbcols_old[c]['order'] ])
        else:
            states[c].append(dbcols_old[c]['order'])

    modcols = list()
    addcols = list()
    logger(__name__).debug((tblname, len(cols_order)))
    for i in sorted(cols_order):
        c = cols_order[i]
        logger(__name__).debug((i, c))
        if len(states[c]) == 2 and len(set(states[c])) == 1:
            continue
        pos = 'first' if i == 1 else 'after `{column}`'.format(column= _find_prev_column(cols_order, i, dbcols_old))
        if len(states[c]) == 1:
            addcols.append('add column `{cname}` {ctype} {desc} {pos}'.format(
                cname= c,
                ctype= dbcols[c]['column type'],
                desc=  _build_column_desc(dbcols[c]['column type']),
                pos=   pos
            ))
        else:
            modcols.append('modify column `{cname}` {ctype} {desc} {pos}'.format(
                cname= c,
                ctype= dbcols[c]['column type'],
                desc=  _build_column_desc(dbcols[c]['column type']),
                pos =  pos
            ))
    modcols = ',\n    '.join(modcols)
    addcols = ',\n    '.join(addcols)
    columns = ',\n    '.join([ x for x in [modcols, addcols] if x != '' ])
    #
    ddlformat = """
alter table `{database}`.`{table}`
    {columns}
    """.strip()
    ddl = ddlformat.format(database= database, table= tblname, columns= columns)
    return ddl
