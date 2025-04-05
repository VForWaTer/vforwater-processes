import logging
import json
import os
from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    'version': '0.9.2',
    'id': 'tool_whiteboxgis',
    'title': {
        'en': 'Whitebox GIS Tool',
        'de': 'Whitebox GIS Werkzeug'
    },
    'description': {
        'en': 'Runs Whitebox GIS operations with support for merging multiple input rasters (e.g. terrain analysis).',
        'de': 'Führt Whitebox GIS-Operationen mit Unterstützung für das Zusammenführen mehrerer Raster durch.'
    },
    'keywords': ['whitebox', 'gis', 'raster', 'merge', 'terrain', 'hydrology'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'Information',
        'href': 'https://github.com/VForWaTer/tool_whiteboxgis',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'raster_files': {
            'title': 'Raster files',
            'description': 'List of input raster (GeoTIFF) files to process and merge.',
            'schema': {
                'type': 'array',
                'items': {'type': 'string'}
            }
        },
        'tool_name': {
            'title': 'Whitebox Tool Name',
            'description': 'Tool to run inside the container (e.g., "hillslope_generator")',
            'schema': {
                'type': 'string',
                'default': 'hillslope_generator'
            }
        },
        'stream_threshold': {
            'title': 'Stream Threshold',
            'description': 'Threshold value for stream extraction',
            'schema': {
                'type': 'number',
                'default': 100.0
            }
        },
        'to_file': {
            'title': 'Write Output to File',
            'description': 'Whether the tool should save output to file',
            'schema': {
                'type': 'boolean',
                'default': True
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
            },
            'example': {
                'value': 'completed',
                'container_status': 'exited'
            }
        }
    },
    'example': {
        'inputs': {
            'tool_name': 'hillslope_generator',
            'raster_files': ['elevation1.tif', 'elevation2.tif'],
            'stream_threshold': 100.0,
            'to_file': True
        }
    }
}


class WhiteboxGISProcessor(BaseProcessor):
    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data, path=None):
        mimetype = 'application/json'
        if path is None:
            path = f'whitebox_{os.urandom(5).hex()}'
        user = data.get('User-Info', 'default')

        raster_files = data.get('raster_files', [])
        tool_name = data.get('tool_name', 'hillslope_generator')
        stream_threshold = data.get('stream_threshold', 100.0)
        to_file = data.get('to_file', True)

        if not raster_files or not isinstance(raster_files, list):
            raise ValueError("Missing or invalid 'raster_files' input.")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

        # Write input.json
        input_dict = {
            "inputs": raster_files
        }
        try:
            with open(f'{host_path_in}/inputs.json', 'w') as f:
                json.dump(input_dict, f, indent=4)

            for i, raster in enumerate(raster_files):
                target = os.path.join(host_path_in, f"elevation{i+1}.tif")
                os.system(f'cp "{raster}" "{target}"')

        except Exception as e:
            raise RuntimeError(f"Error preparing input files: {e}")

        image_name = 'ghcr.io/vforwater/tbr_whitebox:v0.9.1'
        container_name = f'whiteboxgis_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'

        mounts = [
            {'type': 'bind', 'source': host_path_in, 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': host_path_out, 'target': container_out}
        ]

        environment = {
            'TOOL_RUN': tool_name,
            'STREAM_THRESHOLD': str(stream_threshold),
            'TO_FILE': str(to_file)
        }

        command = ["python", "/src/run.py"]
        error = 'none'
        try:
            client = PodmanProcessor.connect(secrets['PODMAN_URI'])
            container = PodmanProcessor.pull_run_image(
                client=client, image_name=image_name,
                container_name=container_name, environment=environment,
                mounts=mounts, network_mode='host', command=command)
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
