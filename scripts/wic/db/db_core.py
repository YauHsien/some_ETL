import wic
from wic.db import key as DBKey
from wic import util as Util
from wic.util.logging import logger
import pymysql, re, traceback



def get_connection(host, port, user, password, database, charset, mod_name):
    logger(mod_name).info('try to connect to host {}, database {}'.format(host, database))
    count = 2
    while True:
        try:
            conn = pymysql.connect(host=    host,
                                   user=    user,
                                   passwd=  password,
                                   db=      database,
                                   port=    port,
                                   charset= charset,
                                   connect_timeout= 30,
                                   local_infile= True)
            return conn
        except:
            traceback.print_exc()
            if count <= 1:
                raise
            else:
                logger(mod_name).info('try to re-connect to host {}, database {}'.format(host, database))
                count = count - 1



def table_exists(conn, dbname, tblname):
    
    sql = """
select * from information_schema.tables
where table_schema = '{dbname}' and 
table_name = '{tblname}';
          """.format(dbname= dbname, tblname= tblname)

    cur = run_sql(conn, sql)
    i = -1
    for i, _ in enumerate(cur):
        break
    cur.close()
    return (i == 0)



def get_variable(conn, var):
    if type(conn) is pymysql.connections.Connection:
        cur = conn.cursor()
        try:
            sql = 'select @@{variable}'.format(variable= var)
            cur.execute(sql)
            return cur.fetchone()[0]
        except Exception as ex:
            traceback.print_exc()



def run_sql(conn, sql, prefix_tag= None):
    if type(conn) is pymysql.connections.Connection:
        cur = conn.cursor()
        sql1 = re.sub(r'(\r\n|\n)', ' ', sql).strip()
        sql2 = sql1[:512]
        try:
            #logger(__name__).debug(sql1)
            logger(__name__).debug('run{prefix_tag}: length {sql_len}; "{sql_part}{sql_rest}"'.format(
                prefix_tag= '' if prefix_tag is None else prefix_tag,
                sql_len=    len(sql1),
                sql_part=   sql2,
                sql_rest=   '...' if len(sql1) > len(sql2) else ''
            ))
            cur.execute(sql)
            return cur
        except Exception as e:
            logger(__name__).error('bad SQL: {}'.format(e))
            traceback.print_exc()



def get_schema(dbconf, table, mod_name):
    if type(dbconf) is dict:
        logger(mod_name).info("get schema '{}.{}'".format(dbconf[DBKey.DB], table))
        sql = "select column_name, ordinal_position, column_type from information_schema.columns where table_schema = '{database}' and table_name = '{table}'".format(database= dbconf[DBKey.DB], table= table)
        #logger(__name__).debug(sql)
        conn = get_connection(dbconf[DBKey.HOST], dbconf[DBKey.PORT],
                              dbconf[DBKey.USER], dbconf[DBKey.PSWD],
                              dbconf[DBKey.DB], dbconf[DBKey.CHARSET], mod_name)
        cur = conn.cursor()
        cur.execute(sql)
        result = dict()
        for _, (cn, order, ct) in enumerate(cur):
            result[cn] = dict([
                ('order', order),
                ('column type', ct)
            ])
        conn.close()
        logger(mod_name).info('{} column{}'.format(len(result), '' if len(result) < 2 else 's'))
        return result



def get_schema_another_DB(dbconf, database, table, mod_name):
    if type(dbconf) is dict:
        logger(mod_name).info('get schema "{}.{}"'.format(database, table))
        sql = "select column_name, ordinal_position, column_type from information_schema.columns where table_schema = '{database}' and table_name = '{table}'".format(database= database, table= table)
        #logger(__name__).debug(sql)
        conn = get_connection(dbconf[DBKey.HOST], dbconf[DBKey.PORT],
                              dbconf[DBKey.USER], dbconf[DBKey.PSWD],
                              dbconf[DBKey.DB], dbconf[DBKey.CHARSET], mod_name)
        with conn.cursor() as cur:
            cur.execute(sql)
            result = dict()
            for _, (cn, order, ct) in enumerate(cur):
                result[cn] = dict([
                    ('order', order),
                    ('column type', ct)
                ])
        conn.close()
        logger(mod_name).info('{} column{}'.format(len(result), '' if len(result) < 2 else 's'))
        return result
    


"""
## rec_proc: a function of arity 1
## - If the argument is not None:
##   - Treat and approach the argument as a record
##   - Put it into inner closure
## - If the argument is None:
##   - Retrieve the accumulated result from inner closure
##   - Return it
"""
def get_data_set(dbconf, task_name, SQL, rec_proc, mod_name):
    if type(dbconf) is dict and Util.is_function(rec_proc) and Util.get_arity(rec_proc) == 1:
        logger(mod_name).info('fetching data set "{}"'.format(task_name))
        conn = get_connection(dbconf[DBKey.HOST], dbconf[DBKey.PORT],
                              dbconf[DBKey.USER], dbconf[DBKey.PSWD],
                              dbconf[DBKey.DB], dbconf[DBKey.CHARSET], mod_name)
        cur = conn.cursor()
        logger(__name__).debug('SQL: {}'.format(SQL))
        cur.execute(SQL)
        logger(__name__).debug('done')
        i = -1
        for i, x in enumerate(cur):
            rec_proc(x)
        conn.close()
        logger(mod_name).info('{} record{}'.format(i + 1, '' if i < 1 else 's'))
        return rec_proc()
