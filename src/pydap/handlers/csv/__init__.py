import os
import csv

from pydap.handlers.lib import BaseHandler
from pydap.model import *


class CSVHandler(BaseHandler):
    def __init__(self, filepath):
        self.filepath = filepath

    def parse(self, ce):
        """
        Parse the constraint expression and return a dataset.

        """
        try:
            fp = open(self.filepath, 'Ur')
            reader = csv.reader(fp)
        except Exception, e:
            message = 'Unable to open file %s: %s' % (self.filepath, e)

        # create the dataset with a sequence
        name = os.path.split(self.filepath)[1]
        dataset = DatasetType(name)
        seq = dataset['sequence'] = SequenceType('sequence')


class CVSSequence(SequenceType):
    """
    A `SequenceType` that reads data from a CSV file.

    """
    def __init__(self, name, filepath, attributes=None, **kwargs):
        SequenceType.__init__(self, name, None, attributes, **kwargs)

        self.filepath = filepath

    def __getitem__(self, key):

