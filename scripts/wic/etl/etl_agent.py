from wic import RESTRICT
from wic import default as Default
from wic.etl.adapter import Adapter
from wic.etl.line_extractor import LineExtractor
from wic.etl.aggregator import Aggregator
from wic.util import file as File
from wic.util import logging
from wic.util.logging import logger
import sys, _io, datetime, csv, traceback
from dateutil import parser as DateParser



class ETLAgent:

    def __init__(self, header, alter, delimiter= ',', newline= None, mod_name= __name__):
        """
        ## header: { column: type }
        ## alter: { column1: column2 }
        """
        self.extractors = dict()
        self.aggregators = dict()
        self.delimiter = delimiter
        self.newline = newline
        self.header = header
        self.alter = alter
        self.infile = None
        self.fin = None
        self.mod_name = mod_name

        self.lps = 1024 # Line-Per-Second
        self.rrps = 1 # Reflash-Rate-Per-Second
        self.prev_datetime = None
        self.prev_lines = 0
        self.start_time = None
        self.RECENT_LINE_COUNT = 3
        self.recent_lines = list()

    def add(self, adapter, name):
        if type(adapter) in set([LineExtractor, Adapter]):
            logger(self.mod_name).info('set {} "{}"'.format(type(adapter), name))
            self.extractors[name] = adapter
        elif type(adapter) in set([Aggregator]):
            logger(self.mod_name).info('set {} "{}"'.format(type(adpater), name))
            self.aggregators[name] = adapter
        else:
            logger(self.mod_name).warning('unknown adapter "{}": {}'.format(name, type(adapter)))

    def _serialize_header(self, delim):
        return delim.join([ '|' + k for k in self.header.keys() ])
        
    def _serialize_line(self, line, delim):
        return delim.join([ '|' + (str(line[k]) if k in line else 'TBD') for k in self.header.keys() ])
        
    def _push_recent_line(self, line):
        if len(self.recent_lines) > self.RECENT_LINE_COUNT - 1:
            self.recent_lines.pop()
        self.recent_lines.append(line)
            
    def _inspect_recent_lines(self, linum):
        lastLinum = linum - self.RECENT_LINE_COUNT + 1
        logger(self.mod_name).error(
            '          {}'.format(self._serialize_header('\t')))
        for i, ln in enumerate(self.recent_lines):
            logger(self.mod_name).error(
                'line {}: {} ({} column{})'.format(
                    lastLinum + i,
                    self._serialize_line(ln, '\t'),
                    len(ln),
                    '' if len(ln) < 2 else 's'
                ))
        
    def _is_init(self):
        case = len(self.extractors) + len(self.aggregators) > 0 and\
               type(self.header) is dict and len(self.header) > 0 and\
               type(self.fin) is _io.TextIOWrapper
        logger(self.mod_name).debug('extractor count: {}'.format(len(self.extractors)))
        logger(self.mod_name).debug('aggregator count: {}'.format(len(self.aggregators)))
        logger(self.mod_name).debug('header type: {}'.format(type(self.header)))
        logger(self.mod_name).debug('column count: {}'.format(len(self.header)))
        logger(self.mod_name).debug('file-in type: {}'.format(type(self.fin)))
        return case

    def _time_log(self, line_count):
        self.prev_datetime = datetime.datetime.now()
        self.prev_lines = line_count
            
    def _data_rate(self, n):
        ##logger(__name__).debug('{} {} lps\n'.format(n, self.lps))
        if n > 0 and int(n / self.lps) == n / self.lps:
            ##logger(__name__).debug('{} {} lps\n'.format(n, self.lps))
            ss = (datetime.datetime.now() - self.prev_datetime).total_seconds()
            ##logger(__name__).debug('{} / {} lps\n'.format(lines, ss))
            if ss > 0:
                lines = n - self.prev_lines
                data_rate = lines / ss
                logging.put_rolling_bar(n, self.lps, '{:,.2f} lps'.format(data_rate))
                self.lps = int(data_rate / self.rrps)
                self._time_log(n)

    def approach(self, ifpath):
        
        if type(self.fin) is not _io.TextIOWrapper and\
           File.is_path(ifpath) and File.exists(ifpath):

            logger(self.mod_name).info('approaching "{}"'.format(str(ifpath)))
            self.infile = ifpath
            if self.newline is None:
                self.fin = open(str(ifpath), 'r')
            else:
                self.fin = open(str(ifpath), 'r', newline= self.newline)

        if not self._is_init():
            logger(self.mod_name).warning('bad ETL agent: agent {} not initialized; skipping it'.format(str(self)))

        self.start_time = datetime.datetime.now()
        self._time_log(0)
        logging.switch_to_progress(self.mod_name)
        reader = csv.DictReader(self.fin, delimiter= RESTRICT.DELIMITER)
        i = -1
        for i, line in enumerate(reader):
            self._push_recent_line(line)
            self._transform(line, linum= i + 1)
            self._data_rate(i)
        logging.switch_to_normal(self.mod_name)
        self.fin.close()

        self._report(i + 1)
        if i > -1:
            self._output()

    def _report(self, linum):
        if linum == 0:
            logger(self.mod_name).info('no line')
        else:
            lps, spl = self._statistics(linum)
            logger(self.mod_name).info('{:,} line{}'.format(linum, '' if linum < 2 else 's'))
            logger(self.mod_name).info('{:,.2f} lps'.format(lps))
            logger(self.mod_name).debug('{:,.6f} spl'.format(spl))

    def _statistics(self, linum):
        ss = (datetime.datetime.now() - self.start_time).total_seconds()
        lps = linum / ss
        spl = ss / linum
        return lps, spl

    def _transform(self, line, linum):
        adapter = None
        try:
            for _, k in enumerate(self.extractors):
                adapter = self.extractors[k]
                adapter.transform(self, line, linum)
            for _, k in enumerate(self.aggregators):
                adapter = self.aggregators[k]
                adapter.transform(self, line, linum)
        except Exception as ex:
            logging.switch_to_normal(__name__)
            logger(self.mod_name).error('in file "{}":'.format(self.infile))
            logger(self.mod_name).error('in line {}'.format(linum))
            logger(__name__).debug('line: {}'.format(line))
            logger(__name__).debug('adapter: {}'.format(adapter))
            logger(__name__).debug('header: {}'.format(adapter.header))
            logger(__name__).debug('line: {}'.format(adapter.line))
            self._inspect_recent_lines(linum)
            traceback.print_exc()
            sys.exit(Default.INTERRUPT)

    def _output(self):
        for _, k in enumerate(self.extractors):
            self.extractors[k].dump_file()
        for _, k in enumerate(self.aggregators):
            self.aggregators[k].dump_file()

    def clean(self):
        self.fin.close()
        self.infile = None
        self.fin = None
        del self.header
        del self.alter
        del self.infile
        del self.fin
        for _, k in enumerate(self.extractors):
            self.extractors[k].clean()
        for _, k in enumerate(self.aggregators):
            self.aggregators[k].clean()
