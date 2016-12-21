from wic import RESTRICT
from wic import util as Util
from wic.util.logging import logger
from typing import Tuple, Callable
import sys



class Adapter:

    def __init__(self, header, outfile, delimiter= ',', line_proc= None, mod_name= __name__):
        """
        ## header: { column: (lambda(self, line, linum) -> value) }
        ## line: { column: acc } # a list of columns
        ## line_proc: lambda(header, line, linum) -> line
        """

        if not Util.is_function(line_proc) or Util.get_arity(line_proc) != 3:
            logger(mod_name).error('bad function {} for {}'.format(line_proc, self))
            sys.exit(RESTRICT.INTERRUPT)
        
        self.header = header
        self.line = None
        self.outfile = outfile
        self.fout = open(str(outfile), 'w')
        self.delimiter = delimiter
        self.line_proc = line_proc
        self.mod_name = mod_name
        self.RECENT_LINE_COUNT = 3
        self.recent_lines = list()
        
        self._write_header()

    def _write_header(self):
        line = self.delimiter.join(self.header.keys())
        self.fout.write(line)
        self.fout.write('\n')

    def _serialize_header(self, delim):
        return delim.join(list(self.header.keys()))
        
    def _serialize_line(self, line, delim):
        return delim.join([ '"{}"'.format(self.line[k]) if ';' in self.line[k] else str(self.line[k])
                            for _, k in enumerate(self.header.keys()) ])
        
    def _new_line(self):
        self.line = dict()
        
    def transform(self, obj, line, linum):
        """
        ## line: { column: value }
        """
        #self._new_line()
        #for column in self.header:
        #    func = self.header[column]
        #    self.line[column] = func(self, line)
        #self._end_line()
        pass

    def _end_line(self):
        line = self._serialize_line(self.line, self.delimiter)
        self.fout.write(line)
        self.fout.write('\n')

    def dump_file(self):
        self.fout.close()
        logger(self.mod_name).info('written: "{}"'.format(self.outfile))

    def clean(self):
        self.outfile = None
        self.fout = None
        del self.header
        del self.line
        del self.outfile
        del self.fout

        
