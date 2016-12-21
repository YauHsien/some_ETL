import wic.etl.db as DB
import pathlib, re
from functools import reduce
import copy
import logging

template_folder_path = pathlib.Path('priv/engine')
t1 = template_find_latest_table_yyyymm_path = template_folder_path.joinpath('sql_module_1.txt')
t2 = template_find_tables_path = template_folder_path.joinpath('sql_module_2.txt')
t3 = template_ensure_table_path = template_folder_path.joinpath('sql_module_3.txt')

def get_sql_with_t1(dbname, tblpart):
    with open(str(t1), 'r') as f:
        fc = reduce(lambda ln, acc: ' {} '.format(ln.strip()) + acc, f.readlines())
        return fc.replace('%%%dbname%%%', dbname).replace('%%%tblpart%%%', tblpart)

def __table_list_to_csv(table_list):
    return ', '.join(['\'{}\''.format(v) for _, v in enumerate(table_list)])

##
## tmap: { tblpart + '_yyyymm': tblpart + '_201607', ... }
def get_sql_with_t2(dbname, tmap):
    tbllist = __table_list_to_csv(list(tmap.keys()))
    with open(str(t2), 'r') as f:
        fc = reduce(lambda ln, acc: ' {} '.format(ln.strip()) + acc, f.readlines())
        logging.info('check if table(s) ({}) exists in database {}'.format(tbllist, dbname))
        return fc.replace('%%%dbname%%%', dbname).replace('%%%tbllist%%%', tbllist)

def get_sql_with_t3(dbname, tblpart, ym):
    with open(str(t3), 'r') as f:
        fc = reduce(lambda ln, acc: ' {} '.format(ln.strip()) + acc, f.readlines())
        return fc.replace('%%%dbname%%%', dbname).replace('%%%tblpart%%%', tblpart).replace('%%%ym%%%', ym)

def __truncate_latest(str1):
    return re.sub(r'_[lL][aA][tT][eE][sS][tT]$', '', str1)
    
## tblname: table name ending with '_latest'
def find_table_with_latest_date(dbname, tblname):
    tblpart = __truncate_latest(tblname)
    sql = re.sub(r'(\r\n|\n)', ' ', get_sql_with_t1(dbname, tblpart))
    r1 = ()
    #print(sql)
    logging.info('find table {}.{}_yyyymm'.format(dbname, tblpart))
    try:
        conn = DB.get_connection()
        conn.query(sql)
        r = conn.use_result()
        #print(type(r), r)
        if r is None:
            logging.info('internal: \'{}\', no result'.format(sql))
            logging.warning('\'{}.{}_yyyymm\' not found'.format(dbname, tblpart))
            conn.close()
            return None
        r1 = r.fetch_row()
        #print(type(r1), r1)
        conn.close()
    except Exception as er:
        #print(type(er), er)
        logging.info('internal: \'{}\', {}'.format(sql, er))
        logging.warning('\'{}.{}_yyyymm\' not found'.format(dbname, tblpart))
        conn.close()
        return None
    #print(type(r1), r1)
    if r1 == ():
        logging.info('table {}.{}_LATEST has no date'.format(dbname, tblpart))
        logging.warning('\'{}.{}_yyyymm\' not found'.format(dbname, tblpart))
        return None
    else:
        (table,), = r1
        logging.info('\'{}.{}_yyyymm\' match table name \'{}\''.format(dbname, tblpart, table))
        return table

## db_tbl_map: {db1, tbl2: tbl1, ...}
def __find_not_existing_tables(db_tbl_map):
    map1 = dict()
    for db, tbl1 in db_tbl_map:
        tbl2 = db_tbl_map[db, tbl1]
        if tbl2 is None:
            continue
        if db not in map1:
            map1[db] = dict({tbl2: tbl1})
        else:
            map1[db][tbl2] = tbl1
    for db in map1:
        sql = re.sub(r'(\r\n|\n)', ' ', get_sql_with_t2(db, map1[db]))
        tbllist = list()
        #logging.info('internal: {}'.format(sql))
        try:
            conn = DB.get_connection()
            conn.query(sql)
            r = conn.use_result()
            #print(type(r), r)
            if r is None:
                logging.warning('not found')
                conn.close()
                continue
            while True:
                r1 = r.fetch_row()
                #print(type(r1), r1)
                if r1 == ():
                    break;
                else:
                    (tbl,), = r1
                    tbllist.append(tbl)
            conn.close()
            for tbl in map1[db]:
                #print(tbl, tbllist)
                if tbl in tbllist:
                    map1[db][tbl] = None
        except Exception as er:
            #print(type(er), er)
            conn.close()
    #logging.info('internal: {} found'.format(map1))
    return map1
    
## transforming db_tbl_map: from {db1, tbl1: tbl2, ...} to {db1, tbl1: sure, tbl2}
def ensure_tables(db_tbl_map):
    map1 = __find_not_existing_tables(db_tbl_map)
    #logging.info('internal: {}'.format(map1))
    for db in map1:
        for tbl2 in map1[db]:
            if map1[db][tbl2] is not None:
                tbl1 = map1[db][tbl2]
                db_tbl_map[db, tbl1] = ensure_table(db, tbl2), tbl2
    #logging.info('internal: {}'.format(db_tbl_map))
    for db, tbl1 in db_tbl_map:
        try:
            _, _ = db_tbl_map[db, tbl1]
        except Exception as e:
            logging.warning('internal: db_tbl_map[{}, {}] = {}: {}'.format(db, tbl1, db_tbl_map[db, tbl1], e))
            db_tbl_map[db, tbl1] = None
    logging.info('internal: table-mapping found: {}'.format(db_tbl_map))
    return db_tbl_map

def ensure_table(db, tbl2):
    x = re.match(r'^(.+)_(\d\d\d\d\d\d)$', tbl2)
    tblpart = x.group(1)
    ym = x.group(2)
    try:
        DB.run_sql(get_sql_with_t3(db, tblpart, ym))
        return True
    except Exception as er:
        return False
