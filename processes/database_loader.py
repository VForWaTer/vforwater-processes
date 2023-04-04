import logging

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
# import os
# import ast
# import sys
# import os
# import uuid as pyuuid
# import re
# import json
# import pandas as pd
# import datetime
# from datetime import datetime as dt

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'database_loader',
    'title': {
        'en': 'database_loader',
        'de': 'Datenbanklader'
    },
    'description': {
        'en': 'A tool for loading data from database.',
        'de': 'Ein Tool zum Laden von Daten aus einer Datenbank.',
    },
    'keywords': ['dataset profiler', 'HTML', 'JSON'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://example.org/process',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'df': {
            'title': 'Data Frame',
            'description': 'Time series',
            'schema': {
                'type': 'timeseries'
            },
            'minOccurs': 1,
            'maxOccurs': 1,
            'metadata': None,  # TODO how to use?
            'keywords': ['full name', 'personal']
        }
    },
    'outputs': {
        'res': {
            'title': 'result path',
            'description': 'output path',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    },
    'example': {
        'inputs': {
            'df': 'World'
        }
    }
}


class DatabaseLoaderProcessor(BaseProcessor):
    """internal V-FOR-WaTer database Loader"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.database_loader.DatabaseLoaderProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/json'
        df = data.get('df')

        if df is None:
            raise ProcessorExecuteError('Cannot process without a dataset')

        res = 'completed'

        outputs = {
            'id': 'res',
            'value': res
        }

        return mimetype, outputs

    def __repr__(self):
        return '<DatabaseLoaderProcessor> completed!'