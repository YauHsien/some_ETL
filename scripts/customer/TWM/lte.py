from wic.RESTRICT import (CUSTOMER_ID, TASKGROUP, LTE, FTP, DATASOURCE, PROTOCOL, HOST, PORT, USER, PSWD, PATH, CTGR, ZIP_FLT, CSV_FLT, DB, CO, OC, CM, DC, FM, PM, DATABASES, CORE, CHARSET, TABLE, PCOFNSRAW, PCOFNGRAW, IMSCSFRAW, IMSHSSRAW, MADNHRRAW, MADODCRAW, IMSDRARAW, XMLNSSRAW, NOKOBWRAW, NOKOMWRAW, NOKIUMRAW)
import re

system_key = {
    CUSTOMER_ID: 'TWM',
    TASKGROUP: LTE,
    DATASOURCE: {
        PROTOCOL: FTP,
        HOST: 'TWM HOST FTP',
        PORT: 65535,
        USER: 'TWM FTP user',
        PSWD: 'TWM FTP password',
        PATH: '_raw_data/LTE',
        CTGR: '{date:%Y%m}/{date:%Y%m%d}',
        ZIP_FLT: {
            DB: re.compile('DB_schema_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            CO: re.compile('CMDLTE_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            OC: re.compile('CMDLTE_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            CM: re.compile('CMDLTE_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            DC: re.compile('CMDLTE_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            FM: re.compile('FX_ALARM_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            PM: re.compile('((PCOFNSRAW|PCOFNGRAW|IMSCSFRAW|IMSHSSRAW|MADNHRRAW|MADODCRAW|IMSDRARAW|XMLNSSRAW|NOKOBWRAW|NOKOMWRAW|NOKIUMRAW)+)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            PCOFNSRAW: re.compile('(PCOFNSRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            PCOFNGRAW: re.compile('(PCOFNGRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            IMSCSFRAW: re.compile('(IMSCSFRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            IMSHSSRAW: re.compile('(IMSHSSRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            MADNHRRAW: re.compile('(MADNHRRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            MADODCRAW: re.compile('(MADODCRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            IMSDRARAW: re.compile('(IMSDRARAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            XMLNSSRAW: re.compile('(XMLNSSRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            NOKOBWRAW: re.compile('(NOKOBWRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            NOKOMWRAW: re.compile('(NOKOMWRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE),
            NOKIUMRAW: re.compile('(NOKIUMRAW)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip', flags= re.IGNORECASE)
        },
        CSV_FLT: {
            DB: re.compile('DB_CO(UL|LU)MNS_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            CO: re.compile('(C_LTE_(CTP|UTP)_COMMON_OBJECT)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            OC: re.compile('(C_LTE_(CTP|UTP)_COMMON_OBJECT)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            CM: re.compile('(C_LTE_(?!(CTP|UTP)).+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            DC: re.compile('(C_LTE_(?!(CTP|UTP)).+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            FM: re.compile('(FX_ALARM)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            PM: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            PCOFNSRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            PCOFNGRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            IMSCSFRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            IMSHSSRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            MADNHRRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            MADODCRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            IMSDRARAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            XMLNSSRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            NOKOBWRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            NOKOMWRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE),
            NOKIUMRAW: re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt', flags= re.IGNORECASE)
        }
    },
    DATABASES: {
        CORE: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB  :    'xinos_config',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        CO: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_core',
            CHARSET: 'utf8',
            TABLE:   'object_control'
        },
        OC: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_core',
            CHARSET: 'utf8',
            TABLE:   'object_check'
        },
        CM: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_CM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        DC: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_CM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        FM: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_FM',
            CHARSET: 'utf8',
            TABLE:   'FX_ALARM'
        },
        PM: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        PCOFNSRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PCO',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        PCOFNGRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PCO',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        IMSCSFRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_IMS',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        IMSHSSRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_IMS',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        MADNHRRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        MADODCRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        IMSDRARAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_IMS',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        XMLNSSRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_PM',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        NOKOBWRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_NOK',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        NOKOMWRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_NOK',
            CHARSET: 'utf8',
            TABLE:   '*'
        },
        NOKIUMRAW: {
            HOST:    'AMB HOST IP',
            PORT:    65535,
            USER:    'custom user name',
            PSWD:    'custom password',
            DB:      'PoC_NOK',
            CHARSET: 'utf8',
            TABLE:   '*'
        }
    }
}
