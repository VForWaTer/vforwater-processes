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
        'en': 'Executes terrain analysis and GIS operations with input.json based configuration as defined in the GitHub repo.',
        'de': 'Führt GIS-Operationen durch, gesteuert über input.json wie im GitHub-Repository definiert.'
    },
    'keywords': ['whitebox', 'gis', 'input.json', 'terrain', 'merge'],
    'links': [{
        'type': 'text/html',
        'rel': 'about',
        'title': 'Information',
        'href': 'https://github.com/VForWaTer/tool_whiteboxgis',
        'hreflang': 'en-US'
    }],
    'inputs': {
        'input_json': {
            'title': 'Input JSON Configuration',
            'description': 'Full JSON object matching the structure used in the GitHub test folder (whitebox_info, merge_tifs, etc.)',
            'schema': {
                'type': 'object'
            }
        },
        'raster_files': {
            'title': 'Raster files',
            'description': 'List of input raster (GeoTIFF) file paths to be copied into /in folder.',
            'schema': {
                'type': 'array',
                'items': {'type': 'string'}
            }
        }
    },
    'outputs': {
        'result': {
            'title': 'Output directory',
            'description': 'Directory with result files',
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
            'input_json': {
                "whitebox_info": {
                    "parameters": {
                        "toFile": True
                    }
                },
                "merge_tifs": {
                    "parameters": {
                        "method": "nn"
                    },
                    "data": {
                        "in_file": "/in"
                    }
                },
                "hillslope_generator": {
                    "parameters": {
                        "stream_threshold": 100
                    },
                    "data": {
                        "dem": "/in/dem.tif"
                    }
                }
            },
            "raster_files": [
                "/data/geoapi_data/in/testuser/testwhitebox/elevation1.tif",
                "/data/geoapi_data/in/testuser/testwhitebox/elevation2.tif"
            ]
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

        input_json = data.get('input_json')
        raster_files = data.get('raster_files', [])

        if not input_json:
            raise ValueError("Missing 'input_json' configuration.")
        if not raster_files or not isinstance(raster_files, list):
            raise ValueError("Missing or invalid 'raster_files' input.")

        secrets = PodmanProcessor.get_secrets()
        host_path_in = f'{secrets["GEOAPI_PATH"]}/in/{user}/{path}'
        host_path_out = f'{secrets["GEOAPI_PATH"]}/out/{user}/{path}'
        server_path_out = f'{secrets["DATA_PATH"]}/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

        try:
            with open(f'{host_path_in}/input.json', 'w') as f:
                json.dump(input_json, f, indent=4)

            # Copy all raster files into /in folder
            for raster_file in raster_files:
                base = os.path.basename(raster_file)
                target_path = os.path.join(host_path_in, base)
                os.system(f'cp "{raster_file}" "{target_path}"')

        except Exception as e:
            raise RuntimeError(f"Error preparing input files: {e}")

        image_name = 'ghcr.io/vforwater/tbr_whitebox:v0.9.2'
        container_name = f'whiteboxgis_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'

        mounts = [
            {'type': 'bind', 'source': host_path_in, 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': host_path_out, 'target': container_out}
        ]

        environment = {}
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
