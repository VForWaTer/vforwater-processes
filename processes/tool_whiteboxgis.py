import logging
import json
import os
from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'whiteboxgis_tool',
    'title': {
        'en': 'Whitebox GIS Tool',
        'de': 'Whitebox GIS Werkzeug'
    },
    'description': {
        'en': 'Runs Whitebox GIS operations like hillslope generation or terrain analysis.',
        'de': 'Führt Whitebox GIS-Operationen wie Hangauswertung oder Terrainanalyse durch.'
    },
    'keywords': ['whitebox', 'gis', 'raster', 'terrain', 'hydrology'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'Information',
        'href': 'https://github.com/VForWaTer/tool_whiteboxgis',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'raster_file': {
            'title': 'Input raster file path',
            'description': 'Path to the input DEM GeoTIFF file.',
            'schema': {
                'type': 'string'
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'tool_name': {
            'title': 'Whitebox Tool Name',
            'description': 'Tool to run inside the container (e.g., "hillslope_generator")',
            'schema': {
                'type': 'string',
                'default': 'hillslope_generator'
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'stream_threshold': {
            'title': 'Stream Threshold',
            'description': 'Threshold value for stream extraction',
            'schema': {
                'type': 'number',
                'default': 100.0
            },
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'to_file': {
            'title': 'Write Output to File',
            'description': 'Whether the tool should save output to file',
            'schema': {
                'type': 'boolean',
                'default': True
            },
            'minOccurs': 0,
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
        user = data.get('User-Info', 'default')

        raster_path = data.get('raster_file')
        tool_name = data.get('tool_name', 'hillslope_generator')
        stream_threshold = data.get('stream_threshold', 100.0)
        to_file = data.get('to_file', True)

        if not raster_path:
            raise ValueError("Missing required 'raster_file'")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

        input_dict = {
            "tool_name": tool_name,
            "parameters": {
                "stream_threshold": stream_threshold,
                "toFile": to_file
            }
        }

        try:
            with open(f'{host_path_in}/input.json', 'w') as f:
                json.dump(input_dict, f, indent=4)
            os.system(f'cp "{raster_path}" "{host_path_in}/dem.tif"')
        except Exception as e:
            raise RuntimeError(f"Error preparing input files: {e}")

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

        environment = {'TOOL_RUN': tool_name}
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
