
import logging
import json
import os
from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'whiteboxgis_tool',
    'title': {
        'en': 'Whitebox GIS Tool',
        'de': 'Whitebox GIS Werkzeug'
    },
    'description': {
        'en': 'Runs Whitebox GIS operations on input raster data.',
        'de': 'Führt Whitebox GIS-Operationen auf Eingabedaten aus.'
    },
    'keywords': ['whitebox', 'gis', 'raster', 'terrain analysis'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'information',
        'href': 'https://github.com/VForWaTer/tool_whiteboxgis',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'input_raster': {
            'title': 'Input Raster Path',
            'description': 'The input raster file path (GeoTIFF).',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        }
    },
    'outputs': {
        'result': {
            'title': 'Output directory',
            'description': 'Directory with output files',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json'
            }
        }
    }
}


class WhiteboxGISProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        mimetype = 'application/json'
        path = f'whitebox_{os.urandom(5).hex()}'

        input_raster = data.get('input_raster', '/data/example_inputs/elevation.tif')
        user = data.get('User-Info', "NO_USER")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_in = f'{secrets["DATA_PATH"]}/in/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

        input_dict = {
            "whiteboxgis_tool": {
                "parameters": {
                    "input_raster": input_raster
                }
            }
        }

        with open(f'{host_path_in}/inputs.json', 'w', encoding='utf-8') as f:
            json.dump(input_dict, f, ensure_ascii=False, indent=4)

        image_name = 'tool_whiteboxgis:latest'
        container_name = f'whiteboxgis_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'

        mounts = [
            {'type': 'bind', 'source': '/data', 'target': '/data', 'read_only': True},
            {'type': 'bind', 'source': server_path_in, 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': server_path_out, 'target': container_out}
        ]

        volumes = {
            host_path_in: {'bind': container_in, 'mode': 'rw'},
            host_path_out: {'bind': container_out, 'mode': 'rw'}
        }

        environment = {}
        network_mode = 'host'
        command = ["python", "/src/run.py"]

        error = 'none'
        try:
            client = PodmanProcessor.connect(secrets['PODMAN_URI'])
            container = PodmanProcessor.pull_run_image(
                client=client, image_name=image_name,
                container_name=container_name, environment=environment,
                mounts=mounts, network_mode=network_mode,
                volumes=volumes, command=command)
        except Exception as e:
            logging.error(f'Error running Podman: {e}')
            error = str(e)

        status = 'failed'
        tool_logs = 'No logs available'
        try:
            container.reload()
            status = container.status
            tool_logs = ''.join(log.decode('utf-8') for log in container.logs())
        except Exception as e:
            logging.error(f'Error getting logs/status: {e}')
            error = f'{error} | Log/Status error: {e}'

        outputs = {
            'container_status': status,
            'value': 'completed' if status == 'exited' else 'failed',
            'dir': server_path_out,
            'error': error,
            'tool_logs': tool_logs
        }

        return mimetype, outputs

    def __repr__(self):
        return '<WhiteboxGISProcessor>'
