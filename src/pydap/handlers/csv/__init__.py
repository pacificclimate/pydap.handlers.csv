import os
import csv
import itertools
import operator
import re
import ast

#from pydap.handlers.lib import BaseHandler
from pydap.model import *
from pydap.lib import encode
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

        >>> from StringIO import StringIO
        >>> import csv
        >>> buf = StringIO()
        >>> writer = csv.writer(buf, quoting=csv.QUOTE_NONNUMERIC)
        >>> writer.writerow(['index', 'temperature', 'site'])
        >>> for row in data:
        ...     writer.writerow(row)
        >>> buf.seek(0)

        >>> seq = CSVSequence('example', buf)
        >>> seq['index'] = CSVBaseType('index')
        >>> seq['temperature'] = CSVBaseType('temperature')
        >>> seq['site'] = CSVBaseType('site')
        >>> for line in seq:
        ...     print line
        [10.0, 15.2, 'Diamond_St']
        [11.0, 13.1, 'Blacktail_Loop']
        [12.0, 13.3, 'Platinum_St']
        [13.0, 12.1, 'Kodiak_Trail']

        >>> for line in seq['temperature', 'site', 'index']:
        ...     print line

    """
    def __init__(self, name, filepath, attributes=None, **kwargs):
        StructureType.__init__(self, name, attributes, **kwargs)
        self.filepath = filepath
        self.queries = []
        self.sequence_level = 1

    def __getitem__(self, key):
        # Return a child with corresponding data.
        if isinstance(key, basestring):
            out = StructureType.__getitem__(self, key)
            index = self.keys().index(key)
            out.data = itertools.imap(operator.itemgetter(index), self)
            return out

        # Return a new `SequenceType`, with requested columns.
        elif isinstance(key, tuple):
            out = self.clone()
            for name in key:
                out[name] = StructureType.__getitem__(self, name).clone()
            return out

        # Return a copy with the added constraints.
        elif isinstance(key, ConstraintExpression):
            out = self.clone()
            out.queries.extend( str(key).split('&') )
            return out

    def __iter__(self):
        if hasattr(self.filepath, 'seek'):
            fp = self.filepath
        else:
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
