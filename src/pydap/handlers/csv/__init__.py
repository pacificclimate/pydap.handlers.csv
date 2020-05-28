import os
import csv
import re
import time
from stat import ST_MTIME
from email.utils import formatdate

from pydap.handlers.lib import BaseHandler, IterData, build_filter
from pydap.model import *
from pydap.lib import quote
from pydap.exceptions import OpenFileError


class CSVHandler(BaseHandler):

    extensions = re.compile(r"^.*\.csv$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try: 
            with open(filepath, 'Ur') as fp:
                reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
                vars = reader.next()
        except Exception as exc:
            message = 'Unable to open file {filepath}: {exc}'.format(filepath=filepath, exc=exc)
            raise OpenFileError(message)

        self.additional_headers.append(
                ('Last-modified', (formatdate(time.mktime(time.localtime(os.stat(filepath)[ST_MTIME]))))))

        # build dataset
        name = os.path.split(filepath)[1]
        self.dataset = DatasetType(name)

        # add sequence and children for each column
        seq = self.dataset['sequence'] = SequenceType('sequence')
        for var in vars:
            seq[var] = BaseType(var)

        # set the data
        seq.data = CSVData(filepath, seq.id, seq.keys())


class CSVData(IterData):
    """
    Emulate a Numpy structured array using CSV files.

    Here's a standard dataset for testing sequential data:

        >>> data = [
        ... (10, 15.2, 'Diamond_St'), 
        ... (11, 13.1, 'Blacktail_Loop'),
        ... (12, 13.3, 'Platinum_St'),
        ... (13, 12.1, 'Kodiak_Trail')]

        >>> import csv
        >>> f = open('test.csv', 'w')
        >>> writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        >>> writer.writerow(['index', 'temperature', 'site'])
        >>> for row in data:
        ...     writer.writerow(row)
        >>> f.close()

    Iteraring over the sequence returns data:

        >>> seq = SequenceType('example')
        >>> seq['index'] = BaseType('index')
        >>> seq['temperature'] = BaseType('temperature')
        >>> seq['site'] = BaseType('site')
        >>> seq.data = CSVData('test.csv', seq.id, ('index', 'temperature', 'site'))

        >>> for line in seq:
        ...     print line
        [10.0, 15.2, 'Diamond_St']
        [11.0, 13.1, 'Blacktail_Loop']
        [12.0, 13.3, 'Platinum_St']
        [13.0, 12.1, 'Kodiak_Trail']

    The order of the variables can be changed:

        >>> for line in seq['temperature', 'site', 'index']:
        ...     print line
        [15.2, 'Diamond_St', 10.0]
        [13.1, 'Blacktail_Loop', 11.0]
        [13.3, 'Platinum_St', 12.0]
        [12.1, 'Kodiak_Trail', 13.0]

    We can iterate over children:

        >>> for line in seq['temperature']:
        ...     print line
        15.2
        13.1
        13.3
        12.1

    We can filter the data:

        >>> for line in seq[ seq.index > 10 ]:
        ...     print line
        [11.0, 13.1, 'Blacktail_Loop']
        [12.0, 13.3, 'Platinum_St']
        [13.0, 12.1, 'Kodiak_Trail']

        >>> for line in seq[ seq.index > 10 ]['site']:
        ...     print line
        Blacktail_Loop
        Platinum_St
        Kodiak_Trail

        >>> for line in seq['site', 'temperature'][ seq.index > 10 ]:
        ...     print line
        ['Blacktail_Loop', 13.1]
        ['Platinum_St', 13.3]
        ['Kodiak_Trail', 12.1]

    Or slice it:

        >>> for line in seq[::2]:
        ...     print line
        [10.0, 15.2, 'Diamond_St']
        [12.0, 13.3, 'Platinum_St']

        >>> for line in seq[ seq.index > 10 ][::2]['site']:
        ...     print line
        Blacktail_Loop
        Kodiak_Trail

        >>> for line in seq[ seq.index > 10 ]['site'][::2]:
        ...     print line
        Blacktail_Loop
        Kodiak_Trail

    """
    def __init__(self, filepath, id, vars, cols=None, selection=None, slice_=None):
        super(CSVData, self).__init__(id, vars, cols, selection, slice_)
        self.filepath = filepath

    def gen(self):
        try:
            fp = open(self.filepath, 'Ur')
        except Exception as exc:
            message = 'Unable to open file {filepath}: {exc}'.format(filepath=self.filepath, exc=exc)
            raise OpenFileError(message)

        reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
        reader.next()  # consume var names
        for row in reader:
            yield row
        fp.close()

    def clone(self):
        return self.__class__(self.filepath, self.id, self.vars[:], 
            self.cols[:], self.selection[:], self.slice[:])

        
def _test():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    _test()

    application = CSVHandler(sys.argv[1])
    from pydap.wsgi.ssf import ServerSideFunctions
    application = ServerSideFunctions(application)
    run_simple('localhost', 8001, application, use_reloader=True)
