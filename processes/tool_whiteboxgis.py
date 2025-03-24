
import logging
import json
import os
import shutil
from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'whiteboxgis_tool',
    'title': {
        'en': 'Whitebox GIS Tool',
        'de': 'Whitebox GIS Werkzeug'
    },
    'description': {
        'en': 'Runs Whitebox GIS operations on input raster data with config.',
        'de': 'Führt Whitebox GIS-Operationen auf Eingabedaten mit Konfiguration aus.'
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
        'config_json': {
            'title': 'Input JSON config file path',
            'description': 'Path to input.json file containing parameters.',
            'schema': {
                'type': 'string'
            }
        },
        'raster_file': {
            'title': 'Input raster file path',
            'description': 'Path to dem.tif file.',
            'schema': {
                'type': 'string'
            }
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
        user = 'default'

        config_path = data.get('config_json')
        raster_path = data.get('raster_file')

        if not config_path or not raster_path:
            raise ValueError("Both 'config_json' and 'raster_file' must be provided")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

        try:
            shutil.copy(config_path, f'{host_path_in}/input.json')
            shutil.copy(raster_path, f'{host_path_in}/dem.tif')
        except Exception as e:
            raise RuntimeError(f"Failed to copy input files: {e}")

        image_name = 'tool_whiteboxgis:latest'
        container_name = f'whiteboxgis_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'

        mounts = [
            {'type': 'bind', 'source': host_path_in, 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': host_path_out, 'target': container_out}
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
