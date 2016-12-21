import context
import wic as __sys
import wic.etl.db as __db

def test_db_config_ProductMode():
    if __sys.sys_config['system mode'] == 'product':
        assert __db.db_config['host'] != None
        assert __db.db_config['port'] != None
        assert __db.db_config['user'] != None
        assert __db.db_config['password'] != None

def test_db_config_DevMode():
    if __sys.sys_config['system mode'] == 'dev':
        assert __db.db_config['host'] != None
        assert __db.db_config['port'] != None
        assert __db.db_config['user'] != None
        assert __db.db_config['password'] != None
        assert __db.db_config['autocommit'] == True
        assert __db.db_config['buffered'] == True

def test_conn():
    sql1 = "create table helloworld ( id int ); insert into helloworld values (1), (2);"
    sql2 = "select * from helloworld;"
    sql3 = "drop table helloworld;"
    sql4 = "show tables like 'helloworld';"
    conn = __db.get_connection()

    try:
        conn.query(sql1)
        conn.close()
    except Exception as err:
        print(1, type(err), err)
        conn.close()

    conn = __db.get_connection()
    try:
        conn.query(sql2)
        #r = conn.store_result()
        r = conn.use_result()
        print(type(r), r)
        i = 0
        while i < 3:
            x = r.fetch_row()
            print(type(x), x)
            i = i + 1
        conn.close()
    except Exception as err:
        print(2, type(err), err)
        conn.close()

    conn = __db.get_connection()
    try:
        conn.query(sql3)
        conn.close()
    except Exception as err:
        print(3, type(err), err)
        conn.close()

    conn = __db.get_connection()
    try:
        conn.query(sql4)
        r = conn.use_result()
        assert r is not None
        assert r.fetch_row() == ()
        conn.close()
    except Exception as err:
        print(4, type(err), err)
        conn.close()

    assert True
