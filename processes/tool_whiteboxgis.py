import logging
import json
import os
from pygeoapi.process.base import BaseProcessor
from processes.podman_processor import PodmanProcessor

PROCESS_METADATA = {
    'version': '0.9.0',
    'id': 'tool_whiteboxgis',
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
                'type': 'string',
                'example': '/in/dem.tif'
            }
        },
        'tool_name': {
            'title': 'Whitebox Tool Name',
            'description': 'Tool to run inside the container (e.g., "hillslope_generator")',
            'schema': {
                'type': 'string',
                'default': 'hillslope_generator',
                'example': 'hillslope_generator'
            }
        },
        'stream_threshold': {
            'title': 'Stream Threshold',
            'description': 'Threshold value for stream extraction',
            'schema': {
                'type': 'number',
                'default': 100.0,
                'example': 100.0
            }
        },
        'to_file': {
            'title': 'Write Output to File',
            'description': 'Whether the tool should save output to file',
            'schema': {
                'type': 'boolean',
                'default': True,
                'example': True
            }
        }
    },
    'outputs': {
        'result': {
            'title': 'Output directory',
            'description': 'Directory with output files',
            'schema': {
                'type': 'object',
                'contentMediaType': 'application/json',
                
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
            'raster_file': '/in/dem.tif',
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
        # path = f'whitebox_{os.urandom(5).hex()}'
        if path is None:
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


       # Use container paths directly to avoid mounting mismatch
#        host_path_in = f'/home/geoapi/in/{user}/{path}'
#        host_path_out = f'/home/geoapi/out/{user}/{path}'
#        server_path_out = f'/data/geoapi_data/out/{user}/{path}'

        os.makedirs(host_path_in, exist_ok=True)
        os.makedirs(host_path_out, exist_ok=True)

#        input_dict = {
#            "tool_name": tool_name,
#            "parameters": {
#                "stream_threshold": stream_threshold,
#                "toFile": to_file
#            }
#        }



        input_dict = {
            tool_name: {
                "parameters": {
                    "stream_threshold": stream_threshold,
                    "toFile": to_file
                },
                "data": {
                    "dem": "/in/dem.tif"
                }
            }
        }

        try:
            with open(f'{host_path_in}/input.json', 'w') as f:
                json.dump(input_dict, f, indent=4)
#            os.system(f'cp "{raster_path}" "{host_path_in}/dem.tif"')
            target_raster_path = os.path.join(host_path_in, "dem.tif")
            os.system(f'cp "{raster_path}" "{target_raster_path}"')

        except Exception as e:
            raise RuntimeError(f"Error preparing input files: {e}")

        print("Host input dir exists:", os.path.exists(host_path_in))
        print("Host input dir content:", os.listdir(host_path_in))
        image_name = 'ghcr.io/vforwater/tbr_whitebox:v0.9.1'
        container_name = f'whiteboxgis_tool_{os.urandom(5).hex()}'
        container_in = '/in'
        container_out = '/out'
#        container_in = f'/data/geoapi_data/in/{user}/{path}'
#        container_out = f'/data/geoapi_data/out/{user}/{path}'
        mounts = [
            {'type': 'bind', 'source': f'/data/geoapi_data/in/{user}/{path}', 'target': container_in, 'read_only': True},
            {'type': 'bind', 'source': f'/data/geoapi_data/out/{user}/{path}', 'target': container_out}
        ]

#        mounts = [
#    {'type': 'bind', 'source': f'/data/geoapi_data/in/{user}/{path}', 'target': f'/home/geoapi/in/{user}/{path}', 'read_only': True},
#    {'type': 'bind', 'source': f'/data/geoapi_data/out/{user}/{path}', 'target': f'/home/geoapi/out/{user}/{path}'}
#]

#        volumes = {
#            host_path_in: {'bind': container_in, 'mode': 'rw'},
#            host_path_out: {'bind': container_out, 'mode': 'rw'}
#        }

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
#                volumes=volumes, 
                command=command)
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




