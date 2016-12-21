import os
import sys
sys.path.insert(1, os.path.abspath('scripts'))
sys.path.insert(2, os.path.abspath('lib'))
import pathlib, logging, copy
import datetime

log_formatter = '%(asctime)-15s [%(levelname)s] %(name)s: %(message)s'
logging.basicConfig(format=log_formatter, level=logging.DEBUG)

class index:
    class ctp:
        CO_GID= { 11: 0, 12: 0 }
        CO_DN= { 11: 8, 12: 8 }
        def list(day):
            return [index.ctp.CO_GID[day], index.ctp.CO_DN[day]]
    class utp:
        CO_GID= { 11: 0, 12: 0 }
        CO_DN= { 11: 14, 12: 14 }
        def list(day):
            return [index.utp.CO_GID[day], index.utp.CO_DN[day]]

listC1 = ['priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC11_2016-07-11.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC1_2016-07-11.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC21_2016-07-11.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC2_2016-07-11.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC3_2016-07-11.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC4_2016-07-11.txt']
"""
CO_GID;CO_OC_ID;CO_SYS_VERSION;CO_DI_TOKEN;CO_TIME_STAMP;CO_STATE;CO_OBJECT_INSTANCE;CO_PARENT_GID;CO_DN;CO_NAME;CO_ADMIN_STATE;CO_MR_GID;CO_SO_GID
"""

listU1 = ['priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC11_2016-07-11.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC1_2016-07-11.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC21_2016-07-11.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC2_2016-07-11.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC3_2016-07-11.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC4_2016-07-11.txt']
"""
CO_GID;CO_INT_ID;CO_PARENT_INT_ID;CO_OC_ID;CO_TC_ID;CO_SC_NAME;CO_DI_TOKEN;CO_STATE;CO_OBJECT_INSTANCE;CO_NAME;CO_OC_VENDOR;CO_OCV_SYS_VERSION;CO_TIME_STAMP;CO_SYSTEM_ID;CO_DN;CO_PARENT_GID;CO_MF_GID;CO_MR_GID;CO_SO_GID;CO_SN_GID;CO_USER_DEF_STATE;CO_USER_DEF_ID;CO_MAIN_HOST;CO_ADMIN_STATE;CO_MAINTENANCE_MODE;CO_DB_TIME_STAMP;CO_LIC_TARGET_ID;CO_EXT_DN
"""

listC2 = ['priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC11_2016-07-12.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC1_2016-07-12.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC21_2016-07-12.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC2_2016-07-12.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC3_2016-07-12.txt',
          'priv/test/twm/C_LTE_CTP_COMMON_OBJECT_LRC4_2016-07-12.txt']
"""
CO_GID;CO_OC_ID;CO_SYS_VERSION;CO_DI_TOKEN;CO_TIME_STAMP;CO_STATE;CO_OBJECT_INSTANCE;CO_PARENT_GID;CO_DN;CO_NAME;CO_ADMIN_STATE;CO_MR_GID;CO_SO_GID
"""

listU2 = ['priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC11_2016-07-12.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC1_2016-07-12.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC21_2016-07-12.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC2_2016-07-12.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC3_2016-07-12.txt',
          'priv/test/twm/C_LTE_UTP_COMMON_OBJECT_LRC4_2016-07-12.txt']
"""
CO_GID;CO_INT_ID;CO_PARENT_INT_ID;CO_OC_ID;CO_TC_ID;CO_SC_NAME;CO_DI_TOKEN;CO_STATE;CO_OBJECT_INSTANCE;CO_NAME;CO_OC_VENDOR;CO_OCV_SYS_VERSION;CO_TIME_STAMP;CO_SYSTEM_ID;CO_DN;CO_PARENT_GID;CO_MF_GID;CO_MR_GID;CO_SO_GID;CO_SN_GID;CO_USER_DEF_STATE;CO_USER_DEF_ID;CO_MAIN_HOST;CO_ADMIN_STATE;CO_MAINTENANCE_MODE;CO_DB_TIME_STAMP;CO_LIC_TARGET_ID;CO_EXT_DN
"""

dfile = 'priv/test/twm/dropped.csv'
afile = 'priv/test/twm/added.csv'

def emit_lines(path, indices):
    with open(str(path.resolve()), 'r') as fo:
        for i, ln in enumerate(fo):
            key = list()
            for i, x in enumerate(ln.split(';')):
                if i in indices:
                    key.append(x)
            yield tuple(key)

def collect_bag(path, indices):
    bag = set()
    for key in emit_lines(path, indices):
        bag.add(key)
    return bag

def take_all(fl_c1, fl_u1, day):
    base = set()
    logging.info('[CTP]')
    for p in fl_c1:
        logging.info('load {}'.format(p))
        bag = collect_bag(pathlib.Path(p), index.ctp.list(day))
        logging.info('{} loaded'.format(len(bag)))
        """
        i = 3
        for x in bag:
            i = i - 1
            if i == 0:
                break;
            logging.info('[inspect] {}'.format(x))
        """
        base = base | bag
    logging.info('[UTP]')
    for p in fl_u1:
        logging.info('load {}'.format(p))
        bag = collect_bag(pathlib.Path(p), index.utp.list(day))
        logging.info('{} loaded'.format(len(bag)))
        base = base | bag
    logging.info('total: {}'.format(len(base)))
    return base

def take_all_inc(fl1, indices1, fl2, indices2):
    dropping = set()
    adding = set()
    for i, p1 in enumerate(fl1):
        p2 = fl2[i]
        
        logging.info('load {}'.format(p1))
        bag1 = collect_bag(pathlib.Path(p1), indices1)
        logging.info('{} item{} loaded'.format(len(bag1), '' if len(bag1) < 2 else 's'))
        
        logging.info('load {}'.format(p2))
        bag2 = collect_bag(pathlib.Path(p2), indices2)
        logging.info('{} item{} loaded'.format(len(bag2), '' if len(bag2) < 2 else 's'))

        logging.info('computing dropping and adding...')
        dropping = dropping | bag1
        adding = adding | bag2
        d1 = copy.copy(dropping)
        dropping = dropping - adding
        adding = adding - d1
        logging.info('holding {} dropped item{}'.format(len(dropping), '' if len(dropping) < 2 else 's'))
        logging.info('holding {} added item{}'.format(len(adding), '' if len(adding) < 2 else 's'))

    return dropping, adding

if __name__ == '__main__':

    """
    ## Bad
    bag12 = take_all(listC2, listU2, 12)
    bag11 = take_all(listC1, listU2, 11)

    logging.info('finding dropped items...')
    dropped = bag11 - bag12
    logging.info('{} item{} dropped'.format(len(dropped), '' if len(dropped) < 2 else 's'))

    logging.info('finding added items...')
    added = bag12 - bag11
    logging.info('{} item{} added'.format(len(added), '' if len(added) < 2 else 's'))

    logging.info('done')
    """

    """
    ## Nice
    """
    dropping, adding = take_all_inc(listC1, index.ctp.list(11), listC2, index.ctp.list(12))
    d1, a1 = take_all_inc(listU1, index.utp.list(11), listU2, index.utp.list(12))
    logging.info('computing result...')
    dropping = dropping | d1
    adding = adding | a1
    d1 = copy.copy(dropping)
    dropping = dropping - adding
    adding = adding - d1
    logging.info('total {} dropped item{}'.format(len(dropping), '' if len(dropping) < 2 else 's'))
    logging.info('total {} added item{}'.format(len(adding), '' if len(adding) < 2 else 's'))

    h = 'TIME,CO_GID,CO_DN\n'
    d = datetime.date.today().strftime('%Y-%m-%d')
    lf = '{},{},{}\n'.format(d, '{}', '{}')

    with open(str(pathlib.Path(dfile).resolve()), 'w') as fo:
        fo.write(h)
        for x in dropping:
            try:
                gid, dn = x
                fo.write(lf.format(gid, dn))
            except Exception as er:
                logging.error('bad record \"{}\" in dropping set: {}'.format(x, er))
        fo.close()
        logging.info('file {} written'.format(dfile))

    with open(str(pathlib.Path(afile).resolve()), 'w') as fo:
        fo.write(h)
        for x in adding:
            try:
                gid, dn = x
                fo.write(lf.format(gid, dn))
            except Exception as er:
                logging.error('bad record \"{}\" in adding set: {}'.format(x, er))
        fo.close()
        logging.info('file {} written'.format(afile))
