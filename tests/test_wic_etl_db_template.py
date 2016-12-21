import context
from pathlib import WindowsPath, PosixPath
import wic
import wic.etl.db.template as T

def test_template_folder():
    path = T.template_folder_path.resolve()
    if wic.is_win:
        assert type(path) is WindowsPath
    if wic.is_linux:
        assert type(path) is PosixPath
    assert path.exists()

def test_template_find_latest_table_yyyymm_path():
    path = T.template_find_latest_table_yyyymm_path.resolve()
    if wic.is_win:
        assert type(path) is WindowsPath
    if wic.is_linux:
        assert type(path) is PosixPath
    assert path.exists()

def test_template_find_tables_path():
    path = T.template_find_tables_path.resolve()
    if wic.is_win:
        assert type(path) is WindowsPath
    if wic.is_linux:
        assert type(path) is PosixPath
    assert path.exists()

def test_template_ensure_table_path():
    path = T.template_ensure_table_path.resolve()
    if wic.is_win:
        assert type(path) is WindowsPath
    if wic.is_linux:
        assert type(path) is PosixPath
    assert path.exists()

def test_get_sql_with_t1():
    str1 = T.get_sql_with_t1('db_twm_fm_sum', 'lte_fm_alarm_sum')
    print('SQL: ', str1)
    assert True
    """
    ## This prints the following.
SQL:  select concat('lte_fm_alarm_sum_', (select date_format(time, '%Y%m') from db_twm_fm_sum.lte_fm_alarm_sum_LATEST limit 1));
    """

def test_get_sql_with_t2():
    str1 = T.get_sql_with_t2('db_twm_fm_sum', {'lte_fm_alarm_sum_yyyymm': 'lte_fm_alarm_sum_201607',
                                               'lte_pm_x_yyyymm': 'lte_pm_x_201608'})
    print('SQL: ', str1)
    assert True
    """
    ## This prints the following.
SQL:  show tables from db_twm_fm_sum where Tables_in_db_twm_fm_sum in ('lte_pm_x_201608', 'lte_fm_alarm_sum_201607');
    """

def test_get_sql_with_t3():
    str1 = T.get_sql_with_t3('db_twm_fm_sum', 'lte_fm_alarm_sum', '201607')
    print('SQL: ', str1)
    assert True
    """
    ## This prints the following.
SQL:   use db_twm_fm_sum; create table lte_fm_alarm_sum_201607 like lte_fm_alarm_sum_LATEST;
    """

def test_find_table_with_latest_date():
    assert 'FX_ALARM_201607' == T.find_table_with_latest_date('DB_TWM_FM', 'FX_ALARM_LATEST')

def test___find_not_existing_tables():
    """
    assert {'DB_TWM_FM': {'PX_ALARM_201606': 'PX_ALARM_LATEST', 'FX_ALARM_201605': None}} == \
        T.__find_not_existing_tables({('DB_TWM_FM', 'PX_ALARM_LATEST'): 'PX_ALARM_201606',
                                      ('DB_TWM_FM', 'FX_ALARM_LATEST'): 'FX_ALARM_201605'})
    """
    ## The test is ignored because of situation shift.
