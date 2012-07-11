import os
import csv
import itertools
import operator

#from pydap.handlers.lib import BaseHandler
from pydap.model import *
from pydap.lib import encode
from pydap.handlers.lib import ConstraintExpression
from pydap.exceptions import OpenFileError


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

    """
    def __init__(self, name, filepath, cols, attributes=None, **kwargs):
        StructureType.__init__(self, name, attributes, **kwargs)
        self.filepath = filepath
        self.cols = cols

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
            out.cols = key
            return out

    def __iter__(self):
        try:
            fp = open(self.filepath, 'Ur')
            reader = csv.reader(fp)
        except Exception, e:
            message = 'Unable to open file %s: %s' % (self.filepath, e)
            raise OpenFileError(message)

        vars_ = reader.next()
        indexes = [ vars_.index(col) for col in self.cols ]

        # prepare data
        data = itertools.ifilter(len, reader)  
        data = itertools.imap(lambda line: map(const, line), data)
        data = itertools.imap(lambda line: [ line[i] for i in indexes ], data)

        for row in data:
            yield row

        fp.close()

    def clone(self):
        out = self.__class__(self.name, self.filepath, self.cols[:],
                self.attributes.copy())
        out.id = self.id
        out.sequence_level = self.sequence_level
        
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


def const(s):
    """Try to evalute value or fallback to string.
    
        >>> const("1")
        1
        >>> const("None")
        'None'

    """
    try:
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return s


if __name__ == '__main__':
    s = CSVSequence('s', '136.csv', ('prec', 'temp', 'wind_dir', 'unknown', 'time', 'wind_speed'))
    for rec in s['temp', 'prec']:
        print rec
