import re

system_key = {
    'TASKGROUP': 'LTE',
    'DATASOURCE': {
        'PROTOCOL': 'ftp',
        'HOST': 'default HOST IP',
        'PORT': 65535,
        'USER': 'default user name',
        'PSWD': 'default password',
        'PATH': '_raw_data/LTE',
        'CTGR': '%Y%m/%Y%m%d',
        'ZIP_FLT': {
            'DB': re.compile('DB_schema_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip'),
            'CO': re.compile('CMDLTE_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip'),
            'OC': re.compile('CMDLTE_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip'),
            'CM': re.compile('CMDLTE_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip'),
            'DC': re.compile('CMDLTE_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip'),
            'FM': re.compile('FX_ALARM_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip'),
            'PM': re.compile('((PCOFNSRAW|PCOFNGRAW|IMSCSFRAW|IMSHSSRAW|MADNHRRAW|MADODCRAW|IMSDRARAW|XMLNSSRAW|NOKOBWRAW|NOKOMWRAW|NOKIUMRAW)+)_RAW_(LRC\d+)_\d\d\d\d-\d\d-\d\d[.]zip')
        },
        'CSV_FLT': {
            'DB': re.compile('DB_CO(UL|LU)MNS_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt'),
            'CO': re.compile('(C_LTE_(CTP|UTP)_COMMON_OBJECT)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt'),
            'OC': re.compile('(C_LTE_(CTP|UTP)_COMMON_OBJECT)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt'),
            'CM': re.compile('(C_LTE_(?!(CTP|UTP)).+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt'),
            'DC': re.compile('(C_LTE_(?!(CTP|UTP)).+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt'),
            'FM': re.compile('(FX_ALARM)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt'),
            'PM': re.compile('(\w+)_LRC\d+_\d\d\d\d-\d\d-\d\d[.]txt')
        }
    },
    'DATABASES': {
        'CORE': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB'  :    'xinos_config',
            'CHARSET': 'utf8',
            'TABLE':   '*'
        },
        'CO': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB':      'PoC_core',
            'CHARSET': 'utf8',
            'TABLE':   'object_control'
        },
        'OC': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB':      'PoC_core',
            'CHARSET': 'utf8',
            'TABLE':   'object_check'
        },
        'CM': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB':      'PoC_CM',
            'CHARSET': 'utf8',
            'TABLE':   '*'
        },
        'DC': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB':      'PoC_CM',
            'CHARSET': 'utf8',
            'TABLE':   '*'
        },
        'FM': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB':      'PoC_FM',
            'CHARSET': 'utf8',
            'TABLE':   'FX_ALARM'
        },
        'PM': {
            'HOST':    'default DB IP',
            'PORT':    3306,
            'USER':    'custom user name',
            'PSWD':    'custom password',
            'DB':      'PoC_PM',
            'CHARSET': 'utf8',
            'TABLE':   '*'
        }
    },
    'CATEGORY': {
        ('LRC1',  'CTP'): 10001,
        ('LRC2',  'CTP'): 10011,
        ('LRC3',  'CTP'): 10021,
        ('LRC4',  'CTP'): 10031,
        ('LRC11', 'CTP'): 10041,
        ('LRC21', 'CTP'): 10051,
        ('LRC1',  'UTP'): 10003,
        ('LRC2',  'UTP'): 10013,
        ('LRC3',  'UTP'): 10023,
        ('LRC4',  'UTP'): 10033,
        ('LRC11', 'UTP'): 10043,
        ('LRC21', 'UTP'): 10053,
        ('NHRC01',  'CTP'): 10001,
        ('NHRC02',  'CTP'): 10011,
        ('NHRC3',  'CTP'): 10021,
        ('NHRC4',  'CTP'): 10031,
        ('NHRC11', 'CTP'): 10041,
        ('NHRC21', 'CTP'): 10051,
        ('NHRC1',  'UTP'): 10003,
        ('NHRC2',  'UTP'): 10013,
        ('NHRC3',  'UTP'): 10023,
        ('NHRC4',  'UTP'): 10033,
        ('NHRC11', 'UTP'): 10043,
        ('NHRC21', 'UTP'): 10053
    }
}
