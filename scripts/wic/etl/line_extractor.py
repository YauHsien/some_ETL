from wic import default as Default
from wic.etl.adapter import Adapter
from wic.util import logging
from wic.util.logging import logger
import sys, traceback, pathlib


#def f(header, line, linum, prefix= 'prefix', lrc_n= 'LRCn'):
#    cols = set(line.keys())
#    ext_cols = set(['_id', '_parent_id', '_level', 'LRC'])
#    return dict([ ('_id', prefix + line['CO_GID']),
#                  ('_parent_id', 'prefix' + line['CO_PARENT_GID']),
#                  ('_level', pathlib.Path(line['CO_DN']).stem.split('-')[0]),
#                  ('LRC', lrc_n)] +
#                [ (c, line[c]) if c != 'CO_SYS_VERSION' else (c, line['CO_OCV_SYS_VERSION']) if c not in cols else (c, line[c])
#                  for _, c in enumerate(header)
#                  if c not in ext_cols and  c in cols
#                ])


class LineExtractor(Adapter):

    def __init__(self, header, outfile, delimiter= ',', line_proc= None, mod_name= __name__):
        super(LineExtractor, self).__init__(header, outfile, delimiter, line_proc, mod_name)
        self.lines = list()

    def transform(self, obj, line, linum):
        """
        ## line: { column: value }
        """
        if len(line) < 2:
            return None
        
        self._new_line()
        self.line = self.line_proc(self.header, line, linum)
        line = self._serialize_line(None, self.delimiter)
        self.lines.append(line)

    def dump_file(self):
        for _, ln in enumerate(self.lines):
            self.fout.write(ln)
            self.fout.write('\n')
        super(LineExtractor, self).dump_file()
