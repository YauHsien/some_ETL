import sys
import argparse
import wic.etl.param as param_core

def parse_args():
    if '-h' in sys.argv or '--help' in sys.argv:
        return None
    else:
        return sys.argv

if __name__ == '__main__':
    parse_args()
