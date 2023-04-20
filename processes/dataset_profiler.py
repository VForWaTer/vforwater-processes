import logging
import json

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
import pandas as pd
from toolbox_runner import list_tools
from toolbox_runner.run import get_remote_image_list

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.2.1',
    'id': 'dataset_profiler',
    'title': {
        'en': 'dataset profiler',
        'de': 'Datensatzprofile'
    },
    'description': {
        'en': 'A tool for generating statistical dataset reports '
              'in HTML and JSON using standardized input and outputs.',
        'de': 'Ein Tool zum Generieren statistischer Datensatzberichte '
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
        df = data.get('df')  # get foldername

        if df is None:
            raise ProcessorExecuteError('Cannot process without a dataset')

        # load all images (podman images!)
        images = get_remote_image_list()

        in_dir = '/home/geoapi/in/' + df
        out_dir = '/home/geoapi/out/' + df

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        metadata = {
            "profile": {
                "data": "/in/" + df + "/dataframe.csv"
            }
        }    
        with open(in_dir + 'parameters.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        # df.to_csv(in_dir+'dataframe.csv')

        for image in images:
            if 'profile' in image:
                os.system(f"podman run -t --rm -it --network=host -v {in_dir}:/in -v {out_dir}:/out {image}")
        res = 'completed'

        # tools = list_tools('ghcr', as_dict=True)
        # prof = tools.get('profile')
        # dataset = pd.read_csv(df)
        # res = prof.run(result_path='out/', data=dataset)

        outputs = {
            'id': 'res',
            'value': res
        }

        return mimetype, outputs

    def __repr__(self):
        return '<DatasetProfilerProcessor> completed!'
