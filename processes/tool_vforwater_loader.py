import logging
import json

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
# from toolbox_runner import list_tools
from toolbox_runner.run import get_remote_image_list

# LOGGER = logging.getLogger(__name__)
logging.basicConfig(filename='vforwater_loader.log', encoding='utf-8', level=logging.DEBUG)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.4.0',
    'id': 'vforwater_loader',
    'title': {
        'en': 'Loader for datasets stored in a metacatalog instance.',
        'de': 'Lader für Datensätze, die in einer Metakatalog-Instanz gespeichert sind.'
    },
    'description': {
        'en': 'This tool will use `metacatalog` to load datasets stored in a metacatalog instance, like V-FOR-WaTer. '
              'The requested datasources will be made available in the output directory of the tool. Areal datasets '
              'will be clipped to the **bounding box** of the reference area and multi-file sources are preselected '
              'to fall into the time range specified. '
              'Note that exact extracts (specific time step, specific area) are not yet supported for areal datasets',
        'de': 'Dieses Tool verwendet `metacatalog`, um Datensätze zu laden, die in einer Metakatalog-Instanz, wie '
              'V-FOR-WaTer, gespeichert sind. Die angeforderten Datenquellen werden im Ausgabeverzeichnis des Tools '
              'zur Verfügung gestellt. Flächendatensätze werden auf den **Begrenzungsrahmen** des Referenzgebiets '
              'zugeschnitten, und Multi-File-Quellen sind so vorausgewählt um in den angegebenen Zeitbereich zu fallen.'
              'Beachten Sie, dass exakte Auszüge (bestimmter Zeitschritt, bestimmtes Gebiet) für Flächendatensätze '
              'noch nicht unterstützt werden.',
    },
    'keywords': ['data loader', 'HTML', 'JSON'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://github.com/VForWaTer/tool_vforwater_loader',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'dataset_ids': {
            'title': 'List of Dataset IDs',
            'description': 'Array of values observed at the given coordinates. '
                           'e.g.: a numpy.ndarray like array([299, 277, ... ])',
            'schema': {
                'type': 'array, number',
                'format': 'integer',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 0,
        },
        'reference_area': {
            'title': 'Coordinates',
            'description': 'The reference area can be any valid GeoJSON POLYGON geometry. Datasets that contain areal '
                           'information will be clipped to this area. Be aware, that some remote sensing datasets may '
                           'have global coverage. If you omit this parameter, the full dataset will be loaded, if the '
                           'hosting server allows it.'
                           'Please make sure, that you only pass one FEATURE. FeatureCollections'
                           'e.g.: a numpy.ndarray like array([[181072, 333611], [181025, 333558], ... ])',
            'schema': {
                'type': 'array, feature',
                'format': 'GEOJSON',
                'required': 'false'
            },
            'minOccurs': 0,  # expect the data is not needed
            'maxOccurs': 1,
        },
        'start_date': {
            'title': 'First point in time',
            'description': 'The start date of the dataset, if a time dimension applies to the dataset.',
            'schema': {
                'type': 'dateTime',
                'format': 'string',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 1,
        },
        'end_date': {
            'title': 'Last point in time',
            'description': 'The end date of the dataset, if a time dimension applies to the dataset.',
            'schema': {
                'type': 'dateTime',
                'format': 'string',
                'required': 'true'
            },
            'minOccurs': 1,  # expect the data is needed
            'maxOccurs': 1,
        },
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
            'dataset_ids': [1, 2, 19],
            'start_date': '2000-01-01T12:00:00+01',
            'end_date': '2014-01-01T12:00:00+01',
            'reference_area': {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[8.9771580802, 47.2703623267],
                                     [13.83504270839, 47.2703623267],
                                     [13.8350427083, 50.5644529365],
                                     [8.9771580802, 50.5644529365],
                                     [8.9771580802, 47.2703623267]]]
                },
                'properties': 'Bayern'
            }
        }
    }
}


class VforwaterLoaderProcessor(BaseProcessor):
    """V FOR Water Loader Processor"""

    def __init__(self, processor_def):
        """
        Initialize object
        :param processor_def: provider definition
        :returns: pygeoapi.process.VforwaterLoaderProcessor
        """
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        logging.info("Started execution of vforwater loader")
        mimetype = 'application/json'
        path = ''

        # load all images (podman images!)
        images = get_remote_image_list()
        logging.info(f"Available images are: {images}")

        # collect inputs
        dataset_ids = data.get('dataset_ids')  # path/name to numpy.ndarray
        start_date = data.get('start_date')  # path/name to numpy.ndarray
        end_date = data.get('end_date')  # integer
        reference_area = data.get('reference_area')  # boolean
        logging.info(f"Got input dataset ids: {dataset_ids},   start date: {start_date},   end date: {end_date},   "
                    f"reference area: {reference_area}")

        # here you could check if required files are given and check format
        if dataset_ids is None or start_date is None or end_date is None:
            raise ProcessorExecuteError('Cannot process without required datasets')

        input_dict = {
            "vforwater_loader": {
                "parameters": {
                    "dataset_ids": dataset_ids,
                    "start_date": start_date,
                    "end_date": end_date,
                    "reference_area": reference_area
                }
            }}

        logging.info(f'Created json input for Mirkos tool: {input_dict}')
        in_dir = '/home/geoapi/in/' + path
        out_dir = '/home/geoapi/out/' + path

        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            logging.info(f'Created output directory at: {out_dir}')

        with open(in_dir + '/inputs.json', 'w', encoding='utf-8') as f:
            json.dump(input_dict, f, ensure_ascii=False, indent=4)

        logging.info(f'wrote json to {in_dir}/inputs.json')

        # df.to_csv(in_dir+'dataframe.csv')s
        for image in images:
            logging.info(f'Found image: {image}')
            if 'tbr_vforwater_loader' in image:
                # os.system(f"docker run --rm -t --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=variogram {image}")
                os.system(f"podman run -t --rm -it --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=tool_vforwater_loader {image}")
            else:
                print('Error in processes - tool_vforwater_loader.py. Cannot load docker image.')
                logging.error('Error in processes - tool_vforwater_loader.py. Cannot load docker image.')

        res = 'completed'

        # tools = list_tools('ghcr', as_dict=True)
        # prof = tools.get('profile')
        # dataset = pd.read_csv(df)
        # res = prof.run(result_path='out/', data=dataset)

        outputs = {
            'id': 'res',
            'value': res,
            'dir': out_dir
        }

        logging.info(f'Finished execution of vforwater loader. return {mimetype, outputs}')
        return mimetype, outputs

    def __repr__(self):
        return '<VforwaterLoaderProcessor> completed!'
