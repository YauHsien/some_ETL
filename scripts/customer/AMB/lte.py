from wic.RESTRICT import (CUSTOMER_ID, TASKGROUP, LTE, FTP, DATASOURCE, PROTOCOL, HOST, PORT, USER, PSWD, PATH, CTGR, ZIP_FLT, CSV_FLT, DB, CO, OC, CM, DC, FM, PM, DATABASES, CORE, CHARSET, TABLE, PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW)
import re

system_key = {
    CUSTOMER_ID: 'AMB',
    TASKGROUP: LTE,
    DATASOURCE: {
        PROTOCOL: FTP,
        HOST: 'AMB HOST IP',
        PORT: 65535,
        USER: 'AMB user name',
        PSWD: 'AMB password',
        PATH: 'Xinos RAW data',
        CTGR: '{date:%Y%m}/{date:%Y-%m-%d}',
        ZIP_FLT: {
            DB: re.compile('DB_schema_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            CO: re.compile('COMMOM_OBJ_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            OC: re.compile('DB_schema_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            CM: re.compile('CMDLTE_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            DC: re.compile('CMDLTE_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            FM: re.compile('FM_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            PM: re.compile('((PCOFNSRAW|PCOFNGRAW|IMSCSFRAW|IMSHSSRAW|MADNHRRAW|MADODCRAW|IMSDRARAW|XMLNSSRAW|NOKOBWRAW|NOKOMWRAW|NOKIUMRAW)+)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            PCOFNSRAW: re.compile('(PCOFNSRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            PCOFNGRAW: re.compile('(PCOFNGRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            IMSCSFRAW: re.compile('(IMSCSFRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            IMSHSSRAW: re.compile('(IMSHSSRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            MADNHRRAW: re.compile('(MADNHRRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            MADODCRAW: re.compile('(MADODCRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            IMSDRARAW: re.compile('(IMSDRARAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            XMLNSSRAW: re.compile('(XMLNSSRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            NOKOBWRAW: re.compile('(NOKOBWRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            NOKOMWRAW: re.compile('(NOKOMWRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            NOKIUMRAW: re.compile('(NOKIUMRAW)_RAW_(NHRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE)
        },
        CSV_FLT: {
            DB: re.compile('DB_CO(UL|LU)MNS_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            CO: re.compile('(C_LTE_(CTP|UTP)_COMMON_OBJECT)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            OC: re.compile('(C_LTE_(CTP|UTP)_COMMON_OBJECT)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            CM: re.compile('(C_LTE_(?!(CTP|UTP)).+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            DC: re.compile('(C_LTE_(?!(CTP|UTP)).+)_HNRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            FM: re.compile('(FX_ALARM)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            PM: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            PCOFNSRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            PCOFNGRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            IMSCSFRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            IMSHSSRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            MADNHRRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            MADODCRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            IMSDRARAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            XMLNSSRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            NOKOBWRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            NOKOMWRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            NOKIUMRAW: re.compile('(\w+)_NHRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE)
        }
    },
    DATABASES: {
        CORE: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB  :    'xinos_config',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        CO: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_core',
            CHARSET: 'utf8',
            TABLE:   'object_control'
        },
        OC: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_core',
            CHARSET: 'utf8',
            TABLE:   'object_check'
        },
        CM: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'db_amb_cmdlte',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        DC: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_CM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        FM: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'db_amb_fm',
            CHARSET: 'utf8',
            TABLE:   'FX_ALARM'
        },
        PM: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        PCOFNSRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'db_amb_pcofnsraw',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        PCOFNGRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PCO',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        IMSCSFRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_IMS',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        IMSHSSRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_IMS',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        MADNHRRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        MADODCRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        IMSDRARAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_IMS',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        XMLNSSRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        NOKOBWRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_NOK',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        NOKOMWRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_NOK',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        NOKIUMRAW: {
            HOST:    'custom HOST IP',
            PORT:    3306,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_NOK',
            CHARSET: 'utf8',
            TABLE:   '*'
        }
    }
}
