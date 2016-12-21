import customer
import wic
from wic import RESTRICT
from wic.RESTRICT import (DATABASES, CO, OC, CM, DC, FM, PM, HOST, PORT, USER, PSWD, DB as DATABASE, CHARSET, TABLE)
from wic import db as DB
from wic.db import key as DBKey
from wic.util.logging import logger
from wic.etl.key import system_key
from wic.etl import column
from wic import util as Util
from wic.util import file as File
from wic.util import json as JSON
import pymysql, copy, re, yaml, json



#def _get_default_config():
#    tmp = { HOST: None, PORT: None, USER: None, PSWD: None,
#            DATABASE: None, CHARSET: None, TABLE: None }
#    return { CO: copy.copy(tmp),
#             OC: copy.copy(tmp),
#             CM: copy.copy(tmp),
#             DC: copy.copy(tmp),
#             FM: copy.copy(tmp),
#             PM: copy.copy(tmp) }



#def _find_config_path():
#    base = wic.find_config_path()
#    return base.joinpath(RESTRICT.DBCONF_FILE_NAME)



def create_config_file(cusid, tech, mod_name):
    path = wic.find_DB_config_file_path()
    #logger(__name__).debug(path)
    if not path.exists():
        File.dump_JSON(path, customer.get_default_DB_config(cusid, tech), mod_name)


        
def get_config(cusid, tech, mod_name):
    path = wic.find_DB_config_file_path()
    try:
        return File.load_JSON(path, mod_name)
    except: # if system is not initialized
        return customer.get_default_DB_config(cusid, tech)


    
def get_computed_config(cusid, tech, mod_name):
    json_data = customer.get_default_DB_config(cusid, tech)
    json_data1 = get_config(cusid, tech, __name__)
    for k1 in json_data1:
        if k1 in json_data and json_data1[k1] is not None:
            cur = json_data[k1]
            if type(cur) is dict:
                for k2 in cur:
                    if k2 in cur and json_data1[k1][k2] is not None:
                        cur[k2] = json_data1[k1][k2]
    return json_data



def set_config(cusid, tech, key, value, mod_name):
    path = wic.find_DB_config_file_path()
    json_data = File.load_JSON(path, mod_name)
    if key is not None:
        m = re.match('(\w+)[.](\w+)', key)
        if m is None:
            key1 = key.upper()
            if key1 in json_data and type(json_data[key1]) is not dict:
                json_data[key1] = value
                File.dump_JSON(path, json_data, mod_name)
        else:
            key1, key2 = m.group(1).upper(), m.group(2).upper()
            if key1 in json_data and key2 in json_data[key1]:
                if key2 == 'TABLE' and key1 in ['PM', 'CM', 'DC']:
                    logger(__name__).warning('{}.TABLE: fixed to any table ("*")'.format(key1))
                    json_data[key1][key2] = '*'
                elif key2 == 'PORT':
                    json_data[key1][key2] = int(value)
                else:
                    json_data[key1][key2] = value
                File.dump_JSON(path, json_data, mod_name)



def list_config(cusid, tech, key, mod_name):
    path = wic.find_DB_config_file_path()
    logger(__name__).info('show "{}"'.format(path))
    json_data = File.load_JSON(path, mod_name)
    return JSON.to_yaml(json_data, key)



def list_computed_config(cusid, tech, key, mod_name):
    json_data = get_computed_config(cusid, tech, mod_name)
    return JSON.to_yaml(json_data, key)



def get_columns(cusid, tech, CAT, tblname= None, mod_name= __name__):
    dbconf = get_computed_config(cusid, tech, mod_name)[CAT]
    return DB.get_schema(dbconf, dbconf[DBKey.TABLE] if tblname is None else tblname, mod_name)


def get_columns_another_DB(cusid, tech, CAT, database, tblname, mod_name= __name__):
    dbconf = get_computed_config(cusid, tech, mod_name)[CAT]
    return DB.get_schema_another_DB(dbconf, database, tblname, mod_name)


def dump_columns(cusid, tech, CAT, tblname= None, mod_name= __name__):
    path = column.find_file_path(cusid, tech, CAT)
    cols = get_columns(cusid, tech, CAT, tblname, mod_name)
    File.dump_JSON(path, cols, mod_name)



def get_schema_by_SQL(cusid, tech, CAT, TempSQL, tblname, mod_name= __name__):
    dbconf = get_computed_config(cusid, tech, mod_name)[CAT]
    return DB.get_schema_by_SQL(dbconf, TempSQL, tblname, mod_name)



def _get_agg_rules(cusid, tech, owner, tblname, mod_name= __name__):
    dbconf = customer.get_default_DB_config(cusid, tech)[RESTRICT.CORE]
    conn = DB.get_connection(dbconf[RESTRICT.HOST],
                             dbconf[RESTRICT.PORT],
                             dbconf[RESTRICT.USER],
                             dbconf[RESTRICT.PSWD],
                             dbconf[RESTRICT.DB],
                             dbconf[RESTRICT.CHARSET], __name__)

    SQL = """
select upper(nc.counter_name_in_view),
    oam.obj_level,
    oam._select,
    oam._left_join,
    oam._where,
    oam._group_by,
    ndt.obj_gid_mapping,
    time_aggregation,
    object_aggregation
from NOKIA_DB_TABLES as ndt, OBJ_AGG_MAPPING as oam, NOKIA_COUNTERS as nc
where owner = '{owner}' and table_name = '{table}'
  and instr(upper(ndt.obj_agg_level), upper(oam.obj_level))
  and instr(upper(nc.raw_measurement_view), upper(ndt.table_name))
  and instr(upper(ndt.owner), upper(nc.adaptation))
order by obj_level;
          """.format(owner= owner.upper(), table= tblname.upper())

    col = dict([ (0, 'colname'),
                 (1, 'obj_level'),
                 (2, 'select'),
                 (3, 'left_join'),
                 (4, 'where'),
                 (5, 'group'),
                 (6, 'ID'),
                 (7, 'ta_func'),
                 (8, 'oa_func') ])

    with DB.run_sql(conn, SQL) as cur:
        result = list()
        for _, x in enumerate(cur):
            row = dict([ (col[i], val) for i, val in enumerate(x) ])
            result.append(row)
    conn.close()
    return result



def get_agg_rules(cusid, tech, owner, tblname, mod_name= __name__):
    rules = _get_agg_rules(cusid, tech, owner, tblname, mod_name)
    levels = list(set([ x['obj_level'] for _, x in enumerate(rules) ]))

    result = dict([('all', dict())] + [ (lv, dict()) for lv in levels ])
    for _, x in enumerate(rules):
        result['all'][x['colname']] = x
        result[x['obj_level']][x['colname']] = x
    
    return result

def get_ym_of_latest(dbconf, tblname, dbname= None):
    conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
    
    with conn.cursor() as cur:
        cur.execute("show tables in `{database}` like '{table}_latest'".format(database= dbconf[DATABASE] if dbname is None else dbname, table= tblname))
        i = -1
        for i, x in enumerate(cur): pass
        if i == -1:
            return None
        else:
            sql = 'select `TIME` from {dbpart}`{table}_latest` limit 1'.format(table= tblname, dbpart= '' if dbname is None else '`{database}`.'.format(database= dbname))
            cur.execute(sql)
            for i, (x,) in enumerate(cur):
                d = Util.extract_date(x)
                if d is not None:
                    return (d.year, d.month)
                break
            return None

def make_sure_DB(dbconf, dbname, conn= None):
    if conn is None:
        conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
        callback = lambda: conn.close()
    else:
        callback = lambda: None
        
    if type(dbname) is str:
        with conn.cursor() as cur:
            cur.execute("show databases like '{database}'".format(database= dbname))
            i = -1
            for i, x in enumerate(cur): pass
            if i == -1:
                cur.execute("create database if not exists `{database}` charset='utf8' collate='utf8_general_ci'".format(database= dbname))
        conn.commit()
        callback()
    elif type(dbname) is list:
        for i, dbname1 in enumerate(dbname):
            make_sure_DB(dbconf, dbname1, conn)
        callback()

def find_DB_agg_table_columns(dbconf, tblname, AggSQL, cols):
    cols1, cols1_order = dict(), dict()
    TmpSQL = 'create temporary table `tmp_{table}` {SQL} limit 0; describe `tmp_{table}`'.format(table= tblname, SQL= AggSQL)
    conn = DB.get_connection(dbconf[HOST], dbconf[PORT], dbconf[USER], dbconf[PSWD], dbconf[DATABASE], dbconf[CHARSET], __name__)
    with DB.run_sql(conn, TmpSQL) as cur:
        cur.nextset()
        for i, (field, dbtype, isnull, key, default, extra,) in enumerate(cur):
            cols1_order[i+1] = field
            if dbtype == 'binary(0)':
                cols1[field] = cols[field]
            else:
                cols1[field] = dict([
                    ('column type', dbtype),
                    ('order',       i + 1)
                ])
    conn.rollback()
    conn.close()
    return cols1, cols1_order
