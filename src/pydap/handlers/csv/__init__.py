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
from pydap.lib import encode, combine_slices, fix_slice
from pydap.handlers.lib import ConstraintExpression
from pydap.exceptions import OpenFileError, ConstraintExpressionError


class CSVHandler(BaseHandler):
    def __init__(self, filepath):
        BaseHandler.__init__(self)
        self.filepath = filepath

        try: 
            fp = open(filepath, 'Ur')
        except Exception, exc:
            message = 'Unable to open file %s: %s' % (self.filepath, exc)
            raise OpenFileError(message)

        reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
        self.cols = reader.next()
        fp.close()

        self.additional_headers.append(
                ('Last-modified', (formatdate(time.mktime(time.localtime(os.stat(self.filepath)[ST_MTIME]))))))

    def parse(self, projection, selection):
        """
        Parse the constraint expression and return a dataset.

        """
        # create the dataset with a sequence
        name = os.path.split(self.filepath)[1]
        dataset = DatasetType(name)
        seq = dataset['sequence'] = CSVSequenceType('sequence', self.filepath)

        # apply selection
        seq.selection.extend(selection)

        # by default, return all columns
        cols = self.cols

        # apply projection
        if projection:
            # fix shorthand notation in projection; some clients will request
            # `child` instead of `sequence.child`.
            for var in projection:
                if len(var) == 1 and var[0][0] != seq.name:
                    token.insert(0, (seq.name, ()))

            # get all slices and apply the first one, since they should be equal
            slices = [ fix_slice(var[0][1], (None,)) for var in projection ]
            seq.slice = slices[0]

            # check that all slices are equal
            if any(slice_ != seq.slice for slice_ in slices[1:]):
                raise ConstraintExpressionError('Slices are not unique!')

            # if the sequence has not been directly requested, return only
            # those variables that were requested
            if all(len(var) == 2 for var in projection):
                cols = [ var[1][0] for var in projection ]

        # add variables
        for col in cols:
            dataset['sequence'][col] = CSVBaseType(col)

        return dataset


class CSVSequenceType(SequenceType):
    """
    A `SequenceType` that reads data from a CSV file.

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

        >>> seq = CSVSequenceType('example', 'test.csv')
        >>> seq['index'] = CSVBaseType('index')
        >>> seq['temperature'] = CSVBaseType('temperature')
        >>> seq['site'] = CSVBaseType('site')

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
    def __init__(self, name, filepath, attributes=None, **kwargs):
        StructureType.__init__(self, name, attributes, **kwargs)
        self.filepath = filepath
        self.selection = []
        self.slice = (slice(None),)
        self.sequence_level = 1

    def __getitem__(self, key):
        # Return a child with corresponding data.
        if isinstance(key, basestring):
            out = StructureType.__getitem__(self, key)
            index = self.keys().index(key)
            out.data = itertools.imap(operator.itemgetter(index), self)

        # Return a new `SequenceType`, with requested columns.
        elif isinstance(key, tuple):
            out = self.clone()
            out._keys = list(key)

        # Return a copy with the added constraints.
        elif isinstance(key, ConstraintExpression):
            out = self.clone()
            out.selection.extend( str(key).split('&') )

        # Slice data.
        else:
            out = self.clone()
            if isinstance(key, int):
                key = slice(key, key+1)
            out.slice = combine_slices(self.slice, (key,))

        return out

    def __setitem__(self, key, item):
        # set data on the child
        SequenceType.__setitem__(self, key, item)
        index = self.keys().index(key)
        item.data = itertools.imap(operator.itemgetter(index), self)

    def __iter__(self):
        try:
            fp = open(self.filepath, 'Ur')
        except Exception, e:
            message = 'Unable to open file %s: %s' % (self.filepath, e)
            raise OpenFileError(message)

        reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
        cols = reader.next()
        indexes = [ cols.index(col) for col in self.keys() ]

        # prepare data
        data = itertools.ifilter(len, reader)  
        data = itertools.ifilter(build_filter(self.selection, cols), data)
        data = itertools.imap(lambda line: [ line[i] for i in indexes ], data)
        data = itertools.islice(data, self.slice[0].start, self.slice[0].stop, self.slice[0].step)

        for row in data:
            yield row

        fp.close()

    # data points to the csv iterator
    data = property(__iter__)

    def clone(self):
        out = self.__class__(self.name, self.filepath, self.attributes.copy())
        out.id = self.id
        out.sequence_level = self.sequence_level

        out.selection = self.selection[:]
        
        # Clone children too.
        for child in self.children():
            out[child.name] = child.clone()
            
        return out
        
        
class CSVBaseType(BaseType):
    """
    A BaseType that returns lazy comparisons.
    
    Comparisons return a `ConstraintExpression` object, so multiple comparisons
    can be saved and evaluated only once.
    
    """
    def __eq__(self, other): return ConstraintExpression('%s=%s' % (self.id, encode(other)))
    def __ne__(self, other): return ConstraintExpression('%s!=%s' % (self.id, encode(other)))
    def __ge__(self, other): return ConstraintExpression('%s>=%s' % (self.id, encode(other)))
    def __le__(self, other): return ConstraintExpression('%s<=%s' % (self.id, encode(other)))
    def __gt__(self, other): return ConstraintExpression('%s>%s' % (self.id, encode(other)))
    def __lt__(self, other): return ConstraintExpression('%s<%s' % (self.id, encode(other)))

    @property
    def dtype(self):
        """
        Peek first value to get type.

        """
        peek = self.data.next()
        self.data = itertools.chain((peek,), self.data)
        return np.array(peek).dtype

    shape = ()

    def __getitem__(self, key):
        """
        Lazy slice of the data.

        """
        if isinstance(key, int):
            key = slice(key, key+1)
        return itertools.islice(self.data, key.start, key.stop, key.step)


def build_filter(selection, cols):
    filters = [ bool ]

    for expression in selection:
        id1, op, id2 = re.split('(<=|>=|!=|=~|>|<|=)', expression, 1)

        # a should be a variable in the children
        name1 = id1.split('.')[-1]
        if name1 in cols:
            a = operator.itemgetter(cols.index(name1))
        else:
            raise ConstraintExpressionError('Invalid constraint expression: "%s" ("%s" is not a valid variable)' % (expression, id1))

        # b could be a variable or constant
        name2 = id2.split('.')[-1]
        if name2 in cols:
            b = operator.itemgetter(cols.index(name2))
        else:
            b = lambda line, name2=name2: ast.literal_eval(name2)

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
    from paste.httpserver import serve

    _test()

    application = CSVHandler(sys.argv[1])
    serve(application, port=8001)
