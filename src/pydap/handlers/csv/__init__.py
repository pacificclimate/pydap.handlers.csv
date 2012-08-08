import os
import csv
import itertools
import operator
import re
import ast

#from pydap.handlers.lib import BaseHandler
from pydap.model import *
from pydap.lib import encode, combine_slices
from pydap.handlers.lib import ConstraintExpression
from pydap.exceptions import OpenFileError, ConstraintExpressionError


#class CSVHandler(BaseHandler):
#    def __init__(self, filepath):
#        self.filepath = filepath
#
#    def parse(self, ce):
#        """
#        Parse the constraint expression and return a dataset.
#
#        """
#        try:
#            fp = open(self.filepath, 'Ur')
#            reader = csv.reader(fp)
#        except Exception, e:
#            message = 'Unable to open file %s: %s' % (self.filepath, e)
#            raise OpenFileError(message)
#
#        # create the dataset with a sequence
#        name = os.path.split(self.filepath)[1]
#        dataset = DatasetType(name)
#        seq = dataset['sequence'] = SequenceType('sequence')


class CSVSequence(SequenceType):
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

        >>> seq = CSVSequence('example', 'test.csv')
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
        self.queries = []
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
            out.queries.extend( str(key).split('&') )

        # Slice data.
        else:
            out = self.clone()
            if isinstance(key, int):
                key = slice(key, key+1)
            out.slice = combine_slices(self.slice, (key,))

        return out

    def __iter__(self):
        try:
            fp = open(self.filepath, 'Ur')
        except Exception, e:
            message = 'Unable to open file %s: %s' % (self.filepath, e)
            raise OpenFileError(message)

        reader = csv.reader(fp, quoting=csv.QUOTE_NONNUMERIC)
        vars_ = reader.next()
        indexes = [ vars_.index(col) for col in self.keys() ]

        # prepare data
        data = itertools.ifilter(len, reader)  
        data = itertools.ifilter(build_filter(self.queries, vars_), data)
        data = itertools.imap(lambda line: [ line[i] for i in indexes ], data)
        data = itertools.islice(data, self.slice[0].start, self.slice[0].stop, self.slice[0].step)

        for row in data:
            yield row

        fp.close()

    def clone(self):
        out = self.__class__(self.name, self.filepath, self.attributes.copy())
        out.id = self.id
        out.sequence_level = self.sequence_level

        out.queries = self.queries[:]
        
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

    def __getitem__(self, key):
        if isinstance(key, int):
            key = slice(key, key+1)
        return itertools.islice(self.data, key.start, key.stop, key.step)


def build_filter(queries, vars_):
    filters = [ bool ]

    for query in queries:
        id1, op, id2 = re.split('(<=|>=|!=|=~|>|<|=)', query, 1)

        # a should be a variable in the children
        name1 = id1.split('.')[-1]
        if name1 in vars_:
            a = operator.itemgetter(vars_.index(name1))
        else:
            raise ConstraintExpressionError('Invalid constraint expression: "%s" ("%s" is not a valid variable)' % (query, id1))

        # b could be a variable or constant
        name2 = id2.split('.')[-1]
        if name2 in vars_:
            b = operator.itemgetter(vars_.index(name2))
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
    _test()
