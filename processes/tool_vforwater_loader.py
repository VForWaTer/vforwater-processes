import logging
import json

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
import os
# from toolbox_runner import list_tools
from toolbox_runner.run import get_remote_image_list

from processes.podman_processor import PodmanProcessor

# LOGGER = logging.getLogger(__name__)
# logging.basicConfig(filename='vforwater_loader.log', encoding='utf-8', level=logging.DEBUG)

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
        logging.info("___________________________ Started execution of vforwater loader ___________________________")
        mimetype = 'application/json'
        path = ''

        # load all images (podman images!)   Not used yet. Maybe for a latter implementation of tools
        images = get_remote_image_list()
        logging.info(f"Available images are: {images}")

        # collect inputs
        try:
            # TODO: improve check of inputs
            timeseries_ids = data.get('timeseries_ids', [])  # path/name to numpy.ndarray
            raster_ids = data.get('raster_ids', [])  # path/name to numpy.ndarray
            start_date = data.get('start_date', '')  # path/name to numpy.ndarray
            end_date = data.get('end_date', '')  # integer
            reference_area = data.get('reference_area', [])
        except Exception as e:
            logging.debug(f"Problem with data.get(): {e}")
            logging.debug(f"timeseries_ids worked: {timeseries_ids}")
            logging.debug(f"raster_ids worked: {raster_ids}")
            logging.debug(f"start_date worked: {start_date}")
            logging.debug(f"end_date worked: {end_date}")
            logging.debug(f"reference_area worked: {reference_area} => data.get() seems to work!")

        logging.info('Data is loaded')

        try:
            if isinstance(reference_area, str):
                reference_area = json.loads(reference_area)

            dataset_ids = []
            if len(timeseries_ids) > 0 and len(raster_ids) > 0:
                dataset_ids = timeseries_ids.extend(raster_ids)
            elif len(raster_ids) > 0:
                dataset_ids = raster_ids
            elif len(timeseries_ids) > 0:
                dataset_ids = timeseries_ids
            else:
                logging.info('The input data is not complete.')
                # raise ProcessorExecuteError('Cannot process without required datasets')
                return json.dumps({'warning': 'Running this tool makes no sense without a timeseries or areal dataset.'})
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
                    "dataset_ids": dataset_ids,
                    "start_date": start_date,
                    "end_date": end_date,
                    "reference_area": reference_area
                }
            }}

        logging.info(f"Input for tool is: {input_dict}.")

        # For testing use no inputs but the example of mirko
        # input_dict['vforwater_loader']['parameters'] = PROCESS_METADATA['example']['inputs']  # job fails
        # input_dict = PROCESS_METADATA['example']['inputs']  # job runs through but no result

        logging.info(f'Created json input for tool: {input_dict}')

        host_path_in = '/home/geoapi/in/' + path  # was in_dir
        host_path_out = '/home/geoapi/out/' + path  # was out_dir

        # if not os.path.exists(out_dir):
        #     os.makedirs(out_dir)
        #     logging.debug(f'Created output directory at: {out_dir}')
        #
        # with open(in_dir + '/inputs.json', 'w', encoding='utf-8') as f:
        #     json.dump(input_dict, f, ensure_ascii=False, indent=4)
        #
        # logging.debug(f'wrote json to {in_dir}/inputs.json')

        # use python podman
        error = 'nothing'
        try:
            secrets = PodmanProcessor.get_secrets()
            image_name = 'ghcr.io/vforwater/tbr_vforwater_loader:latest'
            container_name = 'tool_vforwater_loader'
            container_in = '/in'
            container_out = '/out'
            volumes = {
                host_path_in: {'bind': container_in, 'mode': 'rw'},
                host_path_out: {'bind': container_out, 'mode': 'rw'}
            }

            mounts = [{'type': 'bind', 'source': host_path_in, 'target': container_in},
                      {'type': 'bind', 'source': host_path_out, 'target': container_out}]

            environment = {'METACATALOG_URI':
                               f'postgresql://{secrets["USER"]}@{secrets["HOST"]}:{secrets["PORT"]}/{secrets["DATABASE"]}'}
            network_mode = 'host'
            command = ["python", "/src/run.py"]

            uri = secrets['PODMAN_URI']
            client = PodmanProcessor.connect(uri)

            # get all containers
            for container in client.containers.list():
                print('container list: ', container, container.id, "\n")

            container = PodmanProcessor.pull_run_image(client, image_name, container_name, environment, mounts,
                                                       network_mode, volumes, command)
            container.remove()
        except Exception as e:
            print(f'Error running Podman: {e}')
            logging.error(f'Error running Podman: {e}')
            error = e

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

        outputs = {
            'id': 'res',
            'value': res,
            'dir': host_path_out,
            'error:': error
        }

        logging.info(f'Finished execution of vforwater loader. return {mimetype, outputs}')
        return mimetype, outputs

    def __repr__(self):
        return '<VforwaterLoaderProcessor> completed!'
