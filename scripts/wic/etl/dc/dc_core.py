from wic import RESTRICT
from wic import etl as ETL
from wic.etl import column as Column
from wic.etl import common as Common
from wic.etl import db as ETLDB
from wic.etl.key import system_key
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, csv, re



_cat = RESTRICT.CM
_cat1 = RESTRICT.DC
_re_csv1 = re.compile('^(.+)_(LRC\d+)_(\d\d\d\d-\d\d-\d\d)[.]txt$')
_re_csv2 = re.compile('^(.+)_(NHRC\d+)_(\d\d\d\d-\d\d-\d\d)[.]txt$')




def _csv_filename_to_tblname(csv_filename):
    m = _re_csv1.match(csv_filename)
    if m is None:
        m = _re_csv2.match(csv_filename)
    if m is not None:
        return m.group(1), m.group(1).lower().endswith('_list')



"""
# Commnad: etl check cm date date_to
# date:    <yyyymmdd>
# date_to: <yyyymmdd>
"""
def check(cusid, tech, date, date_to):
    if type(date) is datetime.date and type(date_to) is datetime.date:
        logger(__name__).info('checking CM delta {} from {}'.format(date, date_to))

        conf = ETL.get_computed_config(cusid, tech, __name__)
        dpbase = conf[RESTRICT.DATA_PATH].joinpath('{}/{}/{:%Y%m%d}'.format(cusid, tech, date))
        tfdrpath = dpbase.joinpath('tmp')

        cc = Column.get_check_columns(None, cusid, tech, RESTRICT.CORE, __name__)
        df = '{:%Y-%m-%d}'
        preficies = system_key[RESTRICT.CATEGORY]
        excluded_columns = set(['OBJ_GID', 'NBR', 'LAST_MODIFIED', 'LAST_MODIFIER'])
        """
        #c = 0
        #ig = 0
        """
        ## using key (line['CO_GID'], line['CO_PARENT_GID'], line['CO_DN'])
        for LRC, z1, f1, z2, f2 in Common.extract_info_pair(date, date_to, cusid, tech, _cat, __name__):

            tblname, is_list_tblname = _csv_filename_to_tblname(f1)
            if tblname not in cc:
                continue
            outpath = Common.get_output_path('C_LTE_CMDLTE_DELTA_CHECK_{date:%Y%m}'.format(date= date), f1, cusid, tech, date, _cat1, __name__)
            """
            #logger(__name__).debug(tblname)
            #logger(__name__).debug(outpath)
            #logger(__name__).debug((LRC, z1, f1, z2, f2))
            #continue
            """
            Common.extract_file(z1, f1, tfdrpath, __name__)
            Common.extract_file(z2, f2, tfdrpath, __name__)

            curdict = dict()
            tfpath = tfdrpath.joinpath(f2)
            with open(str(tfpath), 'r') as fo:
                reader = csv.DictReader(fo, delimiter= RESTRICT.DELIMITER)
                for _, line in enumerate(reader):
                    if is_list_tblname:
                        curdict[line['OBJ_GID'], line['NBR']] = line
                    else:
                        curdict[line['OBJ_GID']] = line
                fo.close()
            tfpath.unlink()

            tfpath = tfdrpath.joinpath(f1)
            lines = list()
            with open(str(tfpath), 'r') as fo:
                reader = csv.DictReader(fo, delimiter= RESTRICT.DELIMITER)
                if is_list_tblname:
                    e = enumerate([ line for _, line in enumerate(reader)
                                    if (line['OBJ_GID'], line['NBR']) in curdict ])
                else:
                    e = enumerate([ line for _, line in enumerate(reader)
                                    if line['OBJ_GID'] in curdict ])
                """
                #c1 = 0
                #ig1 = 0
                """
                for _, line in e:
                    t = line
                    if is_list_tblname:
                        y = curdict[line['OBJ_GID'], line['NBR']]
                    else:
                        y = curdict[line['OBJ_GID']]
                    if t != y:
                        """
                        #c1 = c1 + 1
                        """
                        for _, col in enumerate([ x for x in line.keys() if x not in excluded_columns ]):
                            if col in t and col in y and t[col] != y[col]:
                                strtdate = df.format(date)
                                strydate = df.format(date_to)
                                new_line = RESTRICT.DELIMITER.join([
                                    strtdate,
                                    strydate,
                                    strtdate,
                                    str(preficies[LRC, 'CTP']) + line['OBJ_GID'],
                                    line['NBR'] if 'NBR' in line else '0',
                                    col,
                                    y[col],
                                    t[col]
                                ])
                                #logger(__name__).debug(new_line)
                                lines.append(new_line)
                    """
                    #else:
                    #    ig1 = ig1 + 1
                    """
                fo.close()
            tfpath.unlink()
            """
            #logger(__name__).debug(c1)
            #logger(__name__).debug(ig1)
            #c = c + c1
            #ig = ig + ig1
            """
            if lines != list():
                Folder.create(outpath.parent, __name__)
                with open(str(outpath), 'w') as fo:
                    fo.write('TIME;DateY;DateT;_id;NBR;Field;ParamY;ParamT\n')
                    i = -1
                    for i, ln in enumerate(lines):
                        fo.write(ln)
                        fo.write('\n')
                    fo.close()
                    logger(__name__).info('written: "{}"'.format(outpath))
                    logger(__name__).info('{} line{}'.format(i + 1, '' if i < 1 else 's'))
            del curdict
                
        """
        #logger(__name__).debug(c)
        #logger(__name__).debug(ig)
        """



def _SQL_gen_proc(sqlformat, lines, owner, tblname):
    """
    ## sqlformat: 'insert into some_table ({columns}) values {values};'
    ## lines: a list of { column: value }
    """
    cs = "TIME,DateY,DateT,_id,NBR,Field,ParamY,ParamT"
    vss = set()
    for i, ln in enumerate(lines):
        vs = "('{TIME}','{DateY}','{DateT}',{_id},{NBR},'{Field}','{ParamY}','{ParamT}')".format(
            TIME= ln['TIME'],
            DateY= ln['DateY'],
            DateT= ln['DateT'],
            _id= ln['_id'],
            NBR= ln['NBR'],
            Field= ln['Field'],
            ParamY= ln['ParamY'],
            ParamT= ln['ParamT']
        )
        vss.add(vs)
    vss = ','.join(vss)
    #logger(__name__).debug('{} record{} prepared'.format(i + 1, '' if i < 1 else 's'))
    return sqlformat.format(columns= cs, values= vss)
    


def load(cusid, tech, date):
    Common.load(_DDL_proc, _SQL_gen_proc, date, cusid, tech, _cat1, __name__)



def _DDL_proc(owner, tblname):
    return 'CREATE TABLE if not exists `{tblname}` ('.format(tblname= tblname) +\
        """
  `TIME` datetime DEFAULT NULL,
  `DateY` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `DateT` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `_id` bigint NOT NULL,
  `NBR` int NOT NULL,
  `Field` varchar(50) NOT NULL DEFAULT '',
  `ParamY` varchar(50) DEFAULT NULL,
  `ParamT` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`DateY`,`DateT`,`_id`,`NBR`,`Field`),
  UNIQUE KEY `unique_key` (`DateY`,`DateT`,`_id`,`NBR`,`Field`),
  KEY `TIME` (`TIME`),
  KEY `_id` (`_id`),
  KEY `Field` (`Field`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
