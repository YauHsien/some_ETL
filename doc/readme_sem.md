# SQL Execution Module

### �w��

#### Windows �w��

System-wise installation:

1. Install Python3: then you get python3 toolchain.
2. Install virtualenv: then you get a `virtualenv' command.
3. Install Visual Studio 2010 or 2015: for installation of mysql-python.
4. Install MySQL Connector/C: for installation of mysql-python.

Then, make a standalone environment:

1. Make sure the working Python version is 3.x.

2. Change directory to root of the ETL system:
   > cd wicetl/

3. Create a new environment `engine' and install python toolchain:
   > virtualenv engine

4. Switch on the new environment:
   > engine\Scripts\activate.bat

5. Install libraries into the new environment:
   > pip install dateutils
   > pip install mysqlclient_1.3.7-cp35-cp35m-win_amd64.whl
   The mysqlclient-1.3.7-cp35-cp35m-win_amd64.whl is an unofficial package downloaded from http://www.lfd.uci.edu/~gohlke/pythonlibs/

6. Switch off the standalone environment if you want to stop the task:
   > engine\Scripts\deactivate.bat

### ����

#### Windows ����

1. Change directory to root of the ETL system:
   > cd wicetl/

2. Switch on the standalone environment:
   > engine\Scripts\Activate.bat

3. Put SEM Script (SEM is for SQL Execution Module): Let a script `sql1.txt' is put in the folder `~/Downloads/sql1.txt'

4. Read the description and note of SEM:
   > python sql_exec_module.py -h
   usage: sql_exec_module.py [-h] cus_ID path

   SQL Execution Module

   positional arguments:
     cus_ID      Customer ID like "TWM" or "KHS"
     path        File path

   optional arguments:
     -h, --help  show this help message and exit

5. Run the script with `sql1.txt':
   > python sql_exec_module.py twm "~/Downloads/sql1.txt"
   Then several things output here

### ��ƪ��

#### �ӷ��ɮ�

* �ӷ��ɮ׬O�@�h�� <SQL> ���ҡA���� XML �]�����O XML �A�]�� XML �� root �Өӷ��ɮרS�� root �^
* <SQL>###..</SQL> �G�O���ѡC
* <SQL> SQL DML/DDL ���A�ç��a regex \w+[.]\w+_yyyymm ���y�z <SQL> �G�O���w�n�����̫����� SQL �C
* <SQL> SQL DML/DDL �� </SQL>�G�O�@�몺 SQL �C 
* �ܽd�ɮצ�� ../sample/sql1.txt

#### ���浲�G

* ���ѡG�L�X [comment] ����r�C
* ���\���檺 SQL �G�L�X [sql] SQL DML/DDL ����r�C
* �����\���� SQL �G�L�X [*sql] SQL DML/DDL ����r�C
* �ܽd���G��� ../sample/sql1_report.txt
