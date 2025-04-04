# =================================================================
#
# Authors: Marcus Strobl <marcus.strobl@kit.edu>
#
# Copyright (c) 2024 Marcus Strobl
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import json
import os

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

# from toolbox_runner import list_tools
from toolbox_runner.run import get_remote_image_list

from processes.podman_processor import PodmanProcessor
from podman import PodmanClient

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.13.0',
    'id': 'vforwater_loader',
    'title': {
        'en': 'Dataset Loader',
        'de': 'Lader für Datensätze'
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
        'timeseries_ids': {
            'title': 'List of Timeseries',
            'description': 'Values observed at the given coordinates. ',
            'schema': {
                'type': 'timeseries',
                # 'format': 'integer',  # the tool needs an array of integers
                'required': 'false'
            },
            'minOccurs': 0,  # > 0 => expect the data is needed
            'maxOccurs': 0,  # 0 => no limit expects an array
        },
        'raster_ids': {
            'title': 'List of Raster data',
            'description': 'Raster values observed at the given coordinates. ',
            'schema': {
                'type': 'raster',
                # 'format': 'integer',  # the tool needs an array of integers
                'required': 'false'
            },
            'minOccurs': 0,  # 0 => data is not required; >0 => data is required
            'maxOccurs': 0,  # 0 => no limit expects an array -> all except of 1 should expect an array
        },
        'reference_area': {
            'title': 'Coordinates',
            'description': 'The reference area can be any valid GeoJSON POLYGON geometry. Datasets that contain areal '
                           'information will be clipped to this area. Be aware, that some remote sensing datasets may '
                           'have global coverage. If you omit this parameter, the full dataset will be loaded, if the '
                           'hosting server allows it.',
            'schema': {
                'type': 'geometry',
                'format': 'GEOJSON',
                'required': 'false'
            },
            'minOccurs': 0,  # expect the data is not required
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
            'minOccurs': 1,  # expect the data is required
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
            'minOccurs': 1,  # expect the data is required
            'maxOccurs': 1,
        },
        'cell_touches': {
            'title': 'Cell Touches',
            'description':  'If set to true, the tool will only return datasets that have a spatial overlap with the reference area.'
                        'If set to false, the tool will return datasets that have a spatial overlap or touch the reference area.'
                        'If omitted, the default is true.'
                        'Note: This parameter only applies to datasets with a defined spatial scale extent.',
            'schema': {
                'type': 'boolean',
                'default': True,
                'required': 'false'
            },
            'minOccurs': 0,
            'maxOccurs': 1,
    },
        # 'integration': {
        #     'title': 'Define how result is handled on server.',
        #     'description': 'Set if the results should be written to disk. All = can improve processing, '
        #                    'none = slower but with result, XXX = faster but only for workflows'
        #                    'BE AWARE, setting this parameter to XXX might result in no result at all.',
        #     'schema': {
        #         'type': 'string',
        #         'enum': ['none', 'all', 'XXX'],
        #         'default': 'none',
        #         'required': 'true'
        #     },
        # },
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
        mimetype = 'application/json'
        path = f'vfw_loader_{os.urandom(5).hex()}'

        # load all images (podman images!)   Not used yet. Maybe for a latter implementation of tools
        # might still use docker. Fix geoprocessapi
        # images = get_remote_image_list()
        # logging.info(f"Available images are: {images}")

        # ________________ get and prepare input data _________________________
        # TODO: improve check of inputs
        timeseries_ids = data.get('timeseries_ids', [])  # path/name to numpy.ndarray
        raster_ids = data.get('raster_ids', [])  # path/name to numpy.ndarray
        start_date = data.get('start_date', '')  # path/name to numpy.ndarray
        end_date = data.get('end_date', '')  # integer
        reference_area = data.get('reference_area', {})
        # TODO: integration becomes important in future versions, when we have workflows. For now this should be always
        #  none, so the parameter is not available for the user. Uncomment it when needed
        # integration = data.get('integration', 'none')
        cell_touches = data.get('cell_touches', True)
        user = data.get('User-Info', "NO_USER")

        logging.info('Data is loaded')

        try:
            if not isinstance(reference_area, dict):
                reference_area = json.loads(reference_area)

            if 'geometry' in reference_area and isinstance(reference_area['geometry'], str):
                reference_area['geometry'] = json.loads(reference_area['geometry'])

            dataset_ids = timeseries_ids
            dataset_ids.extend(raster_ids)
            if len(dataset_ids) == 0:
                logging.info('The input data is not complete.')
                # raise ProcessorExecuteError('Cannot process without required datasets')
                return json.dumps({'warning': 'Running this tool makes no sense without timeseries or areal dataset.'})
        except Exception as e:
            logging.debug(f"Problem while concatenate data: {e}")

        logging.info(f"Got input dataset ids: {dataset_ids},   start date: {start_date},   end date: {end_date},   "
                     f"reference area: {reference_area}")

        # here you could check if required files are given and check format
        if dataset_ids is None or start_date is None or end_date is None:
            logging.error('Cannot process without required datasets')
            raise ProcessorExecuteError('Cannot process without required datasets')

        input_dict = {
            "vforwater_loader": {
                "parameters": {
                    # "integration": integration,
                    "dataset_ids": dataset_ids,
                    "start_date": start_date,
                    "end_date": end_date,
                    "reference_area": reference_area,
                    "cell_touches": cell_touches
                }
            }}

        logging.info(f"Input for tool is: {input_dict}.")

        # For testing use no inputs but the example of mirko
        # input_dict['vforwater_loader']['parameters'] = PROCESS_METADATA['example']['inputs']  # job fails
        # input_dict = PROCESS_METADATA['example']['inputs']  # job runs through but no result

        logging.info(f'Created json input for tool: {input_dict}')

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'  # path in container (mounted in '/data/geoapi' auf server)
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'  # path in container (mounted in '/data/geoapi' auf server)
        # host_path_in = f'/home/geoapi/in/{user}/{path}'  # path in container (mounted in '/data/geoapi' auf server)
        # host_path_out = f'/home/geoapi/out/{user}/{path}'  # was out_dir

        if not os.path.exists(host_path_in):
            os.makedirs(host_path_in)
            logging.debug(f'Created input directory at: {host_path_in}')

        if not os.path.exists(host_path_out):
            os.makedirs(host_path_out)
            logging.debug(f'Created output directory at: {host_path_out}')

        with open(f'{host_path_in}/inputs.json', 'w', encoding='utf-8') as f:
            json.dump(input_dict, f, ensure_ascii=False, indent=4)

        logging.debug(f'wrote json to {host_path_in}/inputs.json')

        # ________________  prepare data to run container _________________________
        logging.info('Prepare container data')
        # image_name = 'vfwregistry:5000/demo/tool_vforwater_loader:0.1'
        # image_name = 'tool_vforwater_loader:latest' # TODO: used for test; still valid?
        # image_name = 'tool_vforwater_loader:latest'
        image_name = 'ghcr.io/vforwater/tbr_vforwater_loader:v0.13.0'
        container_name = f'tool_vforwater_loader_{os.urandom(5).hex()}'

        container_in = '/in'
        container_out = '/out'

        server_path_in = f'{secrets["DATA_PATH"]}/in/{user}/{path}'  # path in container (mounted in '/data/geoapi' auf server)
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'  # was out_dir

        mounts = [{'type': 'bind', 'source': '/data', 'target': '/data', 'read_only': True},
                  {'type': 'bind', 'source': server_path_in, 'target': container_in, 'read_only': True},
                  {'type': 'bind', 'source': server_path_out, 'target': container_out}]  # mal entfernen in pull run
        logging.info(f'use mounts: {mounts}')

        volumes = {  # sollte funktionieren
            host_path_in: {'bind': container_in, 'mode': 'rw'},  # container_in = '/in'
            host_path_out: {'bind': container_out, 'mode': 'rw'}  # container_out = '/out'
        }
        logging.info(f'use volumes: {volumes}')

        environment = {
            'METACATALOG_URI':
                f'postgresql://{secrets["USER"]}@{secrets["HOST"]}:{secrets["PORT"]}/{secrets["DATABASE"]}'}
        network_mode = 'host'
        command = ["python", "/src/run.py"]

        # ________________  run container _________________________
        # use python podman
        error = 'none'
        try:
            client = PodmanProcessor.connect(secrets['PODMAN_URI'])
            logging.info(f'use client: {client}')

            container = PodmanProcessor.pull_run_image(client=client, image_name=image_name,
                                                       container_name=container_name, environment=environment,
                                                       mounts=mounts, network_mode=network_mode, volumes=volumes,
                                                       command=command)
            logging.info(f'running container: {container}')
        except Exception as e:
            print(f'Error running Podman: {e}')
            logging.error(f'Error running Podman: {e}')
            error = e

        status = 'failed'
        tool_logs = 'Found no logs inside of tool'
        try:  # try to get info about container
            container.reload()
            status = container.status
            logging.info(f"Podman status before remove is {status}")
            logs_generator = container.logs()
            tool_logs = ''.join(log.decode('utf-8') for log in logs_generator)
            # tool_logs = logs.decode('utf-8')
        except Exception as e:
            logging.error(f'Error running Podman: {e}')
            error = f'1: Container Exception: {error} --- 2: Get status Exception {e}'

        # container.remove()
        print("podman run completed!")
        logging.info("Podman run completed!")

        # for development hardcode the image
        # image = "ghcr.io/vforwater/tbr_vforwater_loader"
        # logging.debug(f'Use image: {image}')
        # os.system(
        #     f"podman run -t --rm -it --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=tool_vforwater_loader {image}")
        # for image in images:
        #     logging.debug(f'Found image: {image}')
        #     if 'tbr_vforwater_loader' in image:
        #         # os.system(f"docker run --rm -t --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=variogram {image}")
        #         os.system(f"podman run -t --rm -it --network=host -v {in_dir}:/in -v {out_dir}:/out -e TOOL_RUN=tool_vforwater_loader {image}")
        #     else:
        #         print('Error in processes - tool_vforwater_loader.py. Cannot load docker image.')
        #         logging.error('Error in processes - tool_vforwater_loader.py. Cannot load docker image.')

        res = 'completed'

        # tools = list_tools('ghcr', as_dict=True)
        # prof = tools.get('profile')
        # dataset = pd.read_csv(df)
        # res = prof.run(result_path='out/', data=dataset)
        logging.info(f" - container_status: {type(status), status}")
        logging.info(f" - host_path_out: {type(host_path_out), host_path_out}")
        logging.info(f" - error: {type(error), error}")
        logging.info(f" - tool_logs: {type(tool_logs), tool_logs}")

        outputs = {
            # 'id': 'res',
            'container_status': status,
            'geoapi_status': res,
            'value': res,
            # 'host_dir': host_path_out,
            'dir': server_path_out,
            'error:': error,
            'tool_logs': tool_logs
        }

        logging.info(f'Finished execution of vforwater loader. return {mimetype, outputs}')
        return mimetype, outputs

    def __repr__(self):
        return '<VforwaterLoaderProcessor> completed!'
