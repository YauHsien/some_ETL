import context
from wic.etl.util import *
from os.path import abspath, join, isfile, isdir
from math import pi
import datetime


def test_valid_ip_address_True():
    assert valid_ip_address('127.0.0.1') == True

def test_valid_ip_address_False():
    assert valid_ip_address('hello,world') == False

def test_get_now_dt():
    dt_str = get_now_dt()
    assert is_mysql_dt(dt_str)

def test_get_now_date():
    date_str = get_now_date()
    assert is_mysql_date(date_str)

def test_camelcase_to_underscore_helloWorld():
    assert 'hello_world' == camelcase_to_underscore('helloWorld')

def test_camelcase_to_underscore_HelloWorld():
    assert 'hello_world' == camelcase_to_underscore('HelloWorld')

def test_unique_list():
    assert [1, 2, 3, 4] == unique_list([1, 1, 2, 3, 2, 3, 1, 4])

def test_is_csv_file():
    assert True == is_csv_file(abspath(join('priv', 'test_case.csv')))

def test_progress_bar():
    assert progress_bar(sleep= 0.000001) == None

def test_spinner():
    assert spinner(sleep= 0.000001) == None

def test_is_numerical_True():
    assert True == is_numerical(pi)

def test_is_numerical_False():
    assert False == is_numerical('hello,world')

def test_is_digit_True():
    assert True == is_digit('12345')
    
def test_is_digit_False():
    assert False == is_digit(str(pi))

def test_is_float_True():
    assert True == is_float(str(pi))

def test_is_float_False():
    assert False == is_float('12345')

def test_decimal_random():
    assert True == is_float(decimal_random(1, 10, 3))

def test_is_mysql_dt_True():
    assert True == is_mysql_dt('2014-02-28 01:02:03')

def test_is_mysql_dt_False():
    assert False == is_mysql_dt('2014-02-28 24:60:60')

def test_is_mysql_date_True():
    assert True == is_mysql_date('2016-2-29')

def test_is_mysql_date_False():
    assert False == is_mysql_date('2014-2-30')

def test_format_time_elapsed_Hour():
    assert '1h 10m 3s' == format_time_elapsed(1 * 60 * 60 + 10 * 60 + 3)

def test_format_time_elapsed_Minute():
    assert '13m 25s' == format_time_elapsed(13 * 60 + 25)

def test_format_time_elapsed_Second():
    assert '59s' == format_time_elapsed(59)

def test_extract_date_OK():
    assert '2014-02-28' == extract_date('xyz_2014-02-28-C_LTE_APL_2_SW_DM2_LIST_LRC1_2016-07-03.txt')

def test_extract_date_Alt1():
    assert '2014-02-28' == extract_date('hello,2014--02-28world')

def test_extract_date_Alt2():
    assert datetime.date.today().replace(year= 2014).isoformat() == extract_date('hello,2014--02.28world')
    
def test_extract_date_Failed():
    assert False == extract_date('hello,world')

def test_list_in_list_LeftInRight():
    assert True == list_in_list(['a'], ['a', 'b', 'c'])

def test_list_in_list_NotRightInLeft():
    assert False == list_in_list(['a', 'b', 'c'], ['a'])
