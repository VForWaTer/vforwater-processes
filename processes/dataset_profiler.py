import logging

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from toolbox_runner import list_tools
import pandas as pd

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'dataset_profiler',
    'title': {
        'en': 'dataset profiler',
        'de': 'Datensatzprofile'
    },
    'description': {
        'en': 'A tool for generating statistical dataset reports '
              'in HTML and JSON using standardized input and outputs.',
        'fr': 'Ein Tool zum Generieren statistischer Datensatzberichte '
              'in HTML und JSON unter Verwendung standardisierter Eingaben und Ausgaben.',
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
            'description': 'The csv dataset',
            'schema': {
                'type': 'string'
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
            'description': 'output path to a zip file',
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


class DatasetProfilerProcessor(BaseProcessor):
    """dataset profiler Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.dataset_profiler.DatasetProfilerProcessor
        """

        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):

        mimetype = 'application/json'
        df = data.get('df')

        if df is None:
            raise ProcessorExecuteError('Cannot process without a dataset')

        tools = list_tools('ghcr', as_dict=True)
        prof = tools.get('profile')
        dataset = pd.read_csv(df)
        res = prof.run(result_path='out/', data=dataset)

        outputs = {
            'id': 'res',
            'value': res.path
        }

        return mimetype, outputs

    def __repr__(self):
        return '<DatasetProfilerProcessor> completed!'
