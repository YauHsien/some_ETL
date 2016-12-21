import customer
from wic import RESTRICT
from wic.RESTRICT import (PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW)
from wic import etl as ETL
from wic.etl import common as Common
from wic.etl import ds as DataSource
from wic.etl import db as ETLDB
from wic.etl.ds import ds_column as DSColumn
from wic.etl.etl_agent import ETLAgent
from wic.etl.line_extractor import LineExtractor
from wic.etl import db as DB
from wic.util import file as File
from wic.util import folder as Folder
from wic.util.logging import logger
import datetime, pathlib, re



_cat = RESTRICT.PM
_cases = set([PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW,
              IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW])



def gen_scripts(cusid, tech, date):
    #conf = ETL.get_computed_config(cusid, tech, __name__)
    #datapath = conf[RESTRICT.DATA_PATH]
    #ymdpath = datapath.joinpath('{cusid}/{tech}/{d:%Y%m%d}'.format(cusid= cusid, tech= tech, d= date))
    #logger(__name__).debug('aimed path: {path}'.format(path= ymdpath))

    for _, case in enumerate(_cases):
        gen_specific_scripts(cusid, tech, date, case)



def gen_specific_scripts(cusid, tech, date, case, datapath= None):

    _re_tblname_with_ym = re.compile('^(.+)_(\d\d\d\d\d\d)$')

    def _init_datapath(cusid, tech, datapath):
        if datapath is None:
            conf = ETL.get_computed_config(cusid, tech, __name__)
            return conf[RESTRICT.DATA_PATH]
        else:
            return datapath

    def _tblname_without_ym(tblnameWithYm):
        m = _re_tblname_with_ym.match(tblnameWithYm)
        if m is not None:
            return m.group(1)

    def _ym(tblnameWithYm):
        m = _re_tblname_with_ym.match(tblnameWithYm)
        if m is not None:
            return m.group(2)

    def _partition_agg_columns(dbcols, aggcols_spec, dropping_cols, owner, tblname):
    
        _aggcols_all = set(aggcols_spec['all'].keys())
        if len(_aggcols_all) == 0:
            logger(__name__).warning('skip {}.{}: no aggregation columns'.format(owner, tblname))
            return None

        _first_col = [ aggcols_spec['all'][c] for i, c in enumerate(aggcols_spec['all']) if i == 0 ][0]
        if _first_col['ID'] == '':
            logger(__name__).warning('skip {}.{}: no mapping ID'.format(owner, tblname))
            return None

        result = dict()
        for case in aggcols_spec:

            for k in (set(aggcols_spec[case].keys()) - dbcols):
                aggcols_spec[case].pop(k)
            
            first_col = [ aggcols_spec[case][c] for i, c in enumerate(aggcols_spec[case]) if i == 0 ][0]
            aggcols = aggcols_spec[case]
            select = first_col['select']
            group_by = first_col['group']
            restcols = set([ x for x in (dbcols - set(aggcols.keys()) - set(group_by.split(','))
                                         - set(select.split(',')) - dropping_cols)
                             if not x.endswith('_ID') ])

            result[case] = dict()
            result[case]['aggregation'] = aggcols
            result[case]['select'] = select
            result[case]['group by'] = group_by
            result[case]['rest'] = restcols

        return result

    def _build_agg_columns(case, agg_spec):

        colformat = '{func}({col}) as {col}'
        cols = dict()
        if case in set(['daily', 'hourly']):
            for c in agg_spec:
                cols[c] = colformat.format(func= agg_spec[c]['ta_func'], col= c)
        elif case in set(['object', case]):
            for c in agg_spec:
                cols[c] = colformat.format(func= agg_spec[c]['oa_func'], col= c)
        return cols
    
    def _build_select(case, select, agg, rest):
        if case in set(['daily', 'hourly', 'object', case]):
            return ', '.join([select, ', '.join(agg.values()), rest])

    def _build_group_by(case, group_by):

        if case == 'daily':
            return 'date(TIME)'
        elif case == 'hourly':
            return 'hour(TIME)'
        elif case in set(['object', case]):
            return group_by

    def _build_agg_sql(database, tblname, tblname_ym, agg_case, select, group_by, agg_spec, rest):
        
        _agg =      _build_agg_columns(agg_case, agg_spec)
        _reststr = ', '.join([ 'null as {}'.format(x) for x in rest ])

        sql1format = ' '.join([
            'create temporary table test_{t}_{a}_{ym}',
            '  select {s} from {tym} group by {g} limit 0;'.lstrip(),
            
            'describe test_{t}_{a}_{ym}'
        ])
        sql1 = sql1format.format(
            t=   tblname,
            a=   agg_case,
            ym=  _ym(tblname_ym),
            s=   _build_select(agg_case, select, _agg, _reststr),
            tym= tblname_ym,
            g=   _build_group_by(agg_case, group_by)
        )
        
        _cols = _agg.keys()
        if rest == set():
            c1, c2 = (
                ', '.join([select, (', '.join(_cols)) ]),
                ', '.join([select, (', '.join([ _agg[k] for _, k in enumerate(_cols) ])) ])
            )
        else:
            c1, c2 = (
                ', '.join([select, (', '.join(_cols)) ]),
                ', '.join([select, _reststr, (', '.join([ _agg[k] for _, k in enumerate(_cols) ])) ])
            )

        sql2format = ' '.join([
            'truncate {d}_{a}.{t}_{a}_latest;'
            
            'insert into {d}_{a}.{t}_{a}_latest ({c1})',
            '  select {c2} from {tym} group by {g};'.lstrip(),
            
            'insert into {d}_{a}.{t}_{a}_{ym} ({c1})',
            '  select {c2} from {d}_{a}.{t}_{a}_latest'.lstrip()
        ])
        sql2 =  sql2format.format(
            d=   database,
            t=   tblname,
            a=   agg_case,
            c1=  c1,
            c2=  c2,
            tym= tblname_ym,
            g=   _build_group_by(agg_case, group_by),
            ym=  _ym(tblname_ym)
        )

        logger(__name__).debug(sql1)
        #logger(__name__).debug(sql2)
        return sql1, sql2
    
    if case in _cases:
        datapath = _init_datapath(cusid, tech, datapath)
        casepath = datapath.joinpath('{cusid}/{tech}/{d:%Y%m%d}/{case}'.format(cusid= cusid, tech= tech, d= date, case= case))
        logger(__name__).info('aiming path: {path}'.format(path= casepath))

        dbconf = ETLDB.get_computed_config(cusid, tech, __name__)[case]
        database = dbconf[RESTRICT.DB]

        i = -1
        for i, folder in enumerate([ x for x in casepath.glob('*') if x.is_dir() ]):
            #logger(__name__).debug(folder)

            owner, tblname, tblname_ym = folder.parent.name, _tblname_without_ym(folder.name), folder.name
            #logger(__name__).debug((cusid, tech, owner, tblname_ym, __name__))
            dbcols = set(ETLDB.get_columns(cusid, tech, owner, tblname_ym, __name__).keys())
            aggcols_spec = ETLDB.get_agg_rules(cusid, tech, owner, tblname, __name__)
            #logger(__name__).debug(aggcols_spec['all'])

            dropping_cols = set(['PERIOD_START_TIME', '_id', 'LRC', 'TIME'])
            aggcols = _partition_agg_columns(dbcols, aggcols_spec, dropping_cols, owner, tblname)
            if aggcols is None:
                continue

            sqls = list()
            for k in aggcols:
                
                if k == 'all':
                    for x in ['daily', 'hourly', 'object']:
                        sqls.append(_build_agg_sql(
                            database,
                            tblname,
                            tblname_ym,
                            x,
                            aggcols[k]['select'],
                            aggcols[k]['group by'],
                            aggcols_spec[k],
                            aggcols[k]['rest']
                        ))

                else:
                    sqls.append(_build_agg_sql(
                        database,
                        tblname,
                        tblname_ym,
                        k,
                        aggcols[k]['select'],
                        aggcols[k]['group by'],
                        aggcols_spec[k],
                        aggcols[k]['rest']
                    ))
                #logger(__name__).debug(sqls)
        if i == -1:
            logger(__name__).info('skip')
