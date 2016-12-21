# SQL Execution Module

### 安裝

#### Windows 安裝

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

### 執行

#### Windows 執行

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

### 資料表示

#### 來源檔案

* 來源檔案是一層的 <SQL> 標籤，類似 XML （但不是 XML ，因為 XML 有 root 而來源檔案沒有 root ）
* <SQL>###..</SQL> ：是註解。
* <SQL> SQL DML/DDL 等，並夾帶 regex \w+[.]\w+_yyyymm 的描述 <SQL> ：是指定要找到表格最後日期的 SQL 。
* <SQL> SQL DML/DDL 等 </SQL>：是一般的 SQL 。 
* 示範檔案位於 ../sample/sql1.txt

#### 執行結果

* 註解：印出 [comment] 等文字。
* 成功執行的 SQL ：印出 [sql] SQL DML/DDL 等文字。
* 未成功執行 SQL ：印出 [*sql] SQL DML/DDL 等文字。
* 示範結果位於 ../sample/sql1_report.txt
