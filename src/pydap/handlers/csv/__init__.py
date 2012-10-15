import os
import csv
import itertools
import operator
import re
import ast
import time
from stat import ST_MTIME
from email.utils import formatdate

import numpy as np

from pydap.handlers.lib import BaseHandler
from pydap.model import *
from pydap.lib import encode, combine_slices, fix_slice, quote
from pydap.handlers.lib import ConstraintExpression
from pydap.exceptions import OpenFileError, ConstraintExpressionError


class CSVHandler(BaseHandler):

    extensions = re.compile(r"^.*\.csv$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try: 
            with open(filepath, 'Ur') as fp:
                reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
                vars_ = reader.next()
        except Exception, exc:
            message = 'Unable to open file {filepath}: {exc}'.format(filepath=filepath, exc=exc)
            raise OpenFileError(message)

        self.additional_headers.append(
                ('Last-modified', (formatdate(time.mktime(time.localtime(os.stat(filepath)[ST_MTIME]))))))

        # build dataset
        name = os.path.split(filepath)[1]
        self.dataset = DatasetType(name)

        # add sequence and children for each column
        seq = self.dataset['sequence'] = SequenceType('sequence')
        for var in vars_:
            seq[var] = BaseType(var)

        # set the data
        seq.data = CSVData(filepath, seq.id, tuple(vars_))


class CSVData(object):
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
    shape = ()

    def __init__(self, filepath, id, cols, selection=None, slice_=None):
        self.filepath = filepath
        self.id = id
        self.cols = cols
        self.selection = selection or []
        self.slice = slice_ or (slice(None),)

    def __str__(self):
        if isinstance(self.cols, tuple):
            cols = ','.join(self.cols)
        else:
            cols = self.cols
        return self.id + '@' + self.filepath + '/' + cols + '/' + '&'.join(self.selection) + '/' + str(self.slice[0])

    @property
    def dtype(self):
        peek = iter(self).next()
        return np.array(peek).dtype

    def __iter__(self):
        try:
            fp = open(self.filepath, 'Ur')
        except Exception, exc:
            message = 'Unable to open file {filepath}: {exc}'.format(filepath=self.filepath, exc=exc)
            raise OpenFileError(message)

        reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
        vars_ = reader.next()

        if isinstance(self.cols, tuple):
            cols = self.cols
        else:
            cols = (self.cols,)
        indexes = [ vars_.index(col) for col in cols ]

        # prepare data
        data = itertools.ifilter(len, reader)  
        data = itertools.ifilter(build_filter(self.selection, vars_), data)
        data = itertools.imap(lambda line: [ line[i] for i in indexes ], data)
        data = itertools.islice(data, self.slice[0].start, self.slice[0].stop, self.slice[0].step)

        # return data from a children BaseType, not a Sequence
        if not isinstance(self.cols, tuple):
            data = itertools.imap(operator.itemgetter(0), data)

        for row in data:
            yield row

        fp.close()

    def __getitem__(self, key):
        out = self.clone()

        # return the data for a children
        if isinstance(key, basestring):
            out.id = '{id}.{child}'.format(id=self.id, child=key)
            out.cols = key

        # return a new object with requested columns
        elif isinstance(key, list):
            out.cols = tuple(key)

        # return a copy with the added constraints
        elif isinstance(key, ConstraintExpression):
            out.selection.extend( str(key).split('&') )

        # slice data
        else:
            if isinstance(key, int):
                key = slice(key, key+1)
            out.slice = combine_slices(self.slice, (key,))

        return out

    def clone(self):
        return self.__class__(self.filepath, self.id, self.cols[:],
                self.selection[:], self.slice[:])

    def __eq__(self, other): return ConstraintExpression('%s=%s' % (self.id, encode(other)))
    def __ne__(self, other): return ConstraintExpression('%s!=%s' % (self.id, encode(other)))
    def __ge__(self, other): return ConstraintExpression('%s>=%s' % (self.id, encode(other)))
    def __le__(self, other): return ConstraintExpression('%s<=%s' % (self.id, encode(other)))
    def __gt__(self, other): return ConstraintExpression('%s>%s' % (self.id, encode(other)))
    def __lt__(self, other): return ConstraintExpression('%s<%s' % (self.id, encode(other)))

        
def build_filter(selection, cols):
    filters = [ bool ]

    for expression in selection:
        id1, op, id2 = re.split('(<=|>=|!=|=~|>|<|=)', expression, 1)

        # a should be a variable in the children
        name1 = id1.split('.')[-1]
        if name1 in cols:
            a = operator.itemgetter(cols.index(name1))
        else:
            raise ConstraintExpressionError(
                    'Invalid constraint expression: "{expression}" ("{id}" is not a valid variable)'.format(
                    expression=expression, id=id1))

        # b could be a variable or constant
        name2 = id2.split('.')[-1]
        if name2 in cols:
            b = operator.itemgetter(cols.index(name2))
        else:
            b = lambda line, id2=id2: ast.literal_eval(id2)

        op = {
                '<' : operator.lt,
                '>' : operator.gt,
                '!=': operator.ne,
                '=' : operator.eq,
                '>=': operator.ge,
                '<=': operator.le,
                '=~': lambda a, b: re.match(b, a),
        }[op]

        filter_ = lambda line, op=op, a=a, b=b: op(a(line), b(line))
        filters.append(filter_)

    return lambda line: reduce(lambda x, y: x and y, [f(line) for f in filters])


def _test():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    _test()

    application = CSVHandler(sys.argv[1])
    run_simple('localhost', 8001, application, use_reloader=True)
