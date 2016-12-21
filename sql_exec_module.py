import os
import sys
sys.path.insert(1, os.path.abspath('scripts'))
sys.path.insert(2, os.path.abspath('lib'))
import wic.etl.sqlparser as SEM
from wic.etl.util.file import is_file
import pathlib
import logging
import argparse

def main(cus_ID, path):
    logging.info('processing "{}" for {}'.format(args.cus_ID, args.path))
    path = pathlib.Path(path)
    with open(str(path.resolve()), 'r') as fo:
        SEM.work_and_log(fo, cus_ID)

if __name__ == '__main__':

    parser = argparse.ArgumentParser( description= "SQL Execution Module" )
    parser.add_argument('cus_ID', type= str, help= 'Customer ID like "TWM" or "KHS"')
    parser.add_argument('path', type= str, help= 'File path')
    args = parser.parse_args()

    log_formatter = '%(asctime)-15s [%(levelname)s] %(name)s: %(message)s'
    logging.basicConfig(format=log_formatter, level=logging.DEBUG)

    r = is_file(pathlib.Path(args.path))
    if r != True:
        if r == False:
            logging.info('bad argument "path" "{}": not a file'.format(args.path))
        else:
            logging.error('bad argument "path" "{}": {}'.format(args.path, r))
        parser.print_help()
    else:
        main(args.cus_ID, args.path)
        ## Test Case
        #main('TWM', 'priv/test/sql_module_source.txt')

