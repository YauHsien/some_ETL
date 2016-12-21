import context
import os
import wic.etl.sqlparser as sqlparser
import pathlib

def test_is_syntax_valid_True_OneClosedTag():
    target = '   <SQL>TRUNCATE db_%%%_info.lnbts_area;</SQL>  '
    assert True == sqlparser.is_syntax_valid(str= target, tag_name= 'SQL')

def test_is_syntax_valid_True_TwoClosedTag():
    target = '   <SQL>TRUNCATE db_%%%_info.lnbts_area;</SQL> </SQL> '
    assert True == sqlparser.is_syntax_valid(str= target, tag_name= 'SQL')

def test_is_syntax_valid_False_Empy():
    target = '   TRUNCATE db_%%%_info.lnbts_area;</SQL>  '
    assert False == sqlparser.is_syntax_valid(str= target, tag_name= 'SQL')

def test_is_syntax_valid_False_NotEmpy():
    target = '   <SQL>TRUNCATE db_%%%_info<SQL>.lnbts_area;</SQL>  '
    assert False == sqlparser.is_syntax_valid(str= target, tag_name= 'SQL')


def test_generate_string_ending_with_SQL_tag():
    path = os.path.abspath(os.path.join('priv', 'test', 'sql_module_source.txt'))
    with open(path, 'r') as file:
        for line in sqlparser.generate_string_ending_with_SQL_tag(file):
            #print(line)
            pass
    assert True
    """
    ## print things such as:

<SQL>###################################################;</SQL>
 <SQL>####.update TAC in lnbts_area table####;</SQL>
 <SQL>###################################################;</SQL>
 <SQL> update db_%%%_info.lnbts_area_mcc_mnc_list as A inner join ( select distinct lnbts.CO_NAME as LNBTS_NAME, lnbts.CO_OBJECT_INSTANCE as LNBTS_ID, A.LNCEL_TAC as TAC, A.LNCEL_MCC as MCC, A.LNCEL_MNC as MNC from db_%%%_cmdlte.c_lte_lncel_latest as A left join xinos_db_config.object_control as lnbts on (A._id=lnbts._id) left join xinos_db_config.object_control as mrbts on (mrbts._id=lnbts._parent_id) group by LNBTS_NAME, LNBTS_ID, MCC,MNC) as C using (LNBTS_ID, MCC, MNC) set A.TAC=C.TAC; </SQL>
 <SQL>TRUNCATE db_%%%_info.lnbts_area;</SQL>
    """

def test_generate_SQL_list():
    path = os.path.abspath(os.path.join('priv', 'test', 'sql_module_source.txt'))
    with open(path, 'r') as file:
        for line in sqlparser.generate_SQL_list(file):
            #print(line)
            pass
    assert True
    """
    ## print things such as:
('comment', '###################################################;')
('comment', '####.update TAC in lnbts_area table####;')
('comment', '###################################################;')
('sql', ' update db_%%%_info.lnbts_area_mcc_mnc_list as A inner join ( select distinct lnbts.CO_NAME as LNBTS_NAME, lnbts.CO_OBJECT_INSTANCE as LNBTS_ID, A.LNCEL_TAC as TAC, A.LNCEL_MCC as MCC, A.LNCEL_MNC as MNC from db_%%%_cmdlte.c_lte_lncel_latest as A left join xinos_db_config.object_control as lnbts on (A._id=lnbts._id) left join xinos_db_config.object_control as mrbts on (mrbts._id=lnbts._parent_id) group by LNBTS_NAME, LNBTS_ID, MCC,MNC) as C using (LNBTS_ID, MCC, MNC) set A.TAC=C.TAC;')
('sql', 'TRUNCATE db_%%%_info.lnbts_area;')
    """

def test_generate_table_yyyymm():
    path = pathlib.Path('priv/test/sql_module_source.txt').resolve()
    with open(str(path), 'r') as file:
        for line in sqlparser.generate_table_yyyymm(file):
            #print(line)
            pass
    assert True
    """
    ## print things such as:
('sql', ' INSERT IGNORE into db_%%%_fm_sum.lte_fm_alarm_sum_latest SELECT AA.TIME,_id,sum(countOfAlarmCount) as SumOfAlarmCount,SUBSTRING_INDEX(GROUP_CONCAT(distinct concat(SUPPLEMENTARY_INFO," (",countOfAlarmCount,")") order by SUPPLEMENTARY_INFO ASC),\',\',50) as SUPPLEMENTARY_INFO_COUNT_LIST, SUBSTRING_INDEX(GROUP_CONCAT(distinct concat(SUPPLEMENTARY_INFO) order by SUPPLEMENTARY_INFO ASC),\',\',50) as SUPPLEMENTARY_INFO_LIST FROM (SELECT A.TIME, A._id, A.SUPPLEMENTARY_INFO, count(A.time) as countOfAlarmCount FROM db_%%%_fm.fx_alarm_latest A group by A.TIME,A._id,A.SUPPLEMENTARY_INFO) as AA group by AA.TIME,AA._id;')
('table', ('db_%%%_fm_sum', 'lte_fm_alarm_sum_latest'))
('sql', 'INSERT IGNORE into db_%%%_fm_sum.lte_fm_alarm_sum_yyyymm SELECT * FROM db_%%%_fm_sum.lte_fm_alarm_sum_latest;')
    """

def test_customer_tag_replaced():
    path = pathlib.Path('priv/test/sql_module_source.txt').resolve()
    with open(str(path), 'r') as file:
        for line in sqlparser.generate_lines_with_customer_tag_replaced(file, 'twm'):
            #print(line)
            pass
    assert True
    """
('sql', ' INSERT IGNORE into db_twm_fm_sum.lte_fm_alarm_sum_latest SELECT AA.TIME,_id,sum(countOfAlarmCount) as SumOfAlarmCount,SUBSTRING_INDEX(GROUP_CONCAT(distinct concat(SUPPLEMENTARY_INFO," (",countOfAlarmCount,")") order by SUPPLEMENTARY_INFO ASC),\',\',50) as SUPPLEMENTARY_INFO_COUNT_LIST, SUBSTRING_INDEX(GROUP_CONCAT(distinct concat(SUPPLEMENTARY_INFO) order by SUPPLEMENTARY_INFO ASC),\',\',50) as SUPPLEMENTARY_INFO_LIST FROM (SELECT A.TIME, A._id, A.SUPPLEMENTARY_INFO, count(A.time) as countOfAlarmCount FROM db_twm_fm.fx_alarm_latest A group by A.TIME,A._id,A.SUPPLEMENTARY_INFO) as AA group by AA.TIME,AA._id;')
('table', ('db_twm_fm_sum', 'lte_fm_alarm_sum_latest'))
('sql', 'INSERT IGNORE into db_twm_fm_sum.lte_fm_alarm_sum_yyyymm SELECT * FROM db_twm_fm_sum.lte_fm_alarm_sum_latest;')
    """
