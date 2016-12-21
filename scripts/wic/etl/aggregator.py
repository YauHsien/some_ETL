from wic.etl.adapter import Adapter
from wic.util.logging import logger



class Aggregator(Adapter):

    def __ini__(self, keys, aggregation, appendices, outfile, delimiter= ',', mod_name= __name__):
        """
        ## keys: { column: (lambda(self, line) -> value) }
        ## aggregation: { column: (lambda(self, line) -> value) }
        ## appendices: { column: (lambda(self, line) -> value) }
        ## accounts: collected items { group: { column: acc } }
        """
        self.keys = keys
        self.aggregation = aggregation
        self.appendices = appendices
        self.accounts = dict()
        self.fout = open(str(outfile), 'w')
        self.delimiter = delimiter
        self.mod_name

    def _get_group(self, line):
        """
        ## line: { column: value }
        """
        group = dict([ (c, (self.keys[c])(self, line))
                       for c in self.keys if c in line ])
        if len(group) == len(self.keys):
            return group
        else:
            logger(self.mod_name).warning('bad group: "{}" not match "{}"'.format(group, str(list(self.keys.keys()))))
            return None

    def _get_account_by_group(self, group, line):
        """
        ## group: { column: value }
        ## line: { column: value }
        """
        key = tuple(group.values())
        if key not in self.accounts:
            self.accounts[key] = dict()
            account = self.accounts[key]
            for k in group:
                account[k] = group[k]
            for k in self.aggregation:
                account[k] = 0
            for k in self.appendices:
                account[k] = None
        return self.accounts[key]
        
    def transform(self, line):
        """
        ## line: { column: value }
        """
        group = self._get_group(line)
        if group is None:
            return None
        account = self._get_account_by_group(group, line)
        
        for k in self.aggregation:
            f = self.aggregation[k]
            account[k] = f(self, line)

        for k in self.appendices:
            f = self.appendices[k]
            account[k] = f(self, line)

    def dump_file(self):
        header = list(self.keys.keys()) + list(self.aggregation.keys()) + list(self.appendices.keys())
        line = self.delimiter.join(header)
        self.fout.write(line)
        for grp in self.accounts:
            line = self.delimter.join([ self.accounts[grp][c] for c in enumerate(header) ])
            self.fout.write(line)
        self.fout.close()
